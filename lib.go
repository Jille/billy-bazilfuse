// Package billybazilfuse exposes a github.com/bazil/fuse/fs.FS that passes calls to a Billy API.
package billybazilfuse

import (
	"context"
	"errors"
	"io"
	"os"
	"path"
	"sync"
	"syscall"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/go-git/go-billy/v5"
)

// CallHook is the callback you can get before every call from FUSE, before it's passed to Billy.
type CallHook func(ctx context.Context, req fuse.Request) error

// New creates a fuse/fs.FS that passes all calls through to the given filesystem.
// callHook is called before every call from FUSE, and can be nil.
func New(underlying billy.Basic, callHook CallHook) fs.FS {
	if callHook == nil {
		callHook = func(ctx context.Context, req fuse.Request) error {
			return nil
		}
	}
	return &root{
		underlying: underlying,
		callHook:   callHook,
	}
}

type root struct {
	underlying billy.Basic
	callHook   CallHook
}

func (r *root) Root() (fs.Node, error) {
	return &node{r, ""}, nil
}

type node struct {
	root *root
	path string
}

var _ fs.Node = &node{}
var _ fs.NodeCreater = &node{}
var _ fs.NodeMkdirer = &node{}
var _ fs.NodeOpener = &node{}
var _ fs.NodeReadlinker = &node{}
var _ fs.NodeRemover = &node{}
var _ fs.NodeRenamer = &node{}
var _ fs.NodeRequestLookuper = &node{}
var _ fs.NodeSymlinker = &node{}

func (n *node) Attr(ctx context.Context, attr *fuse.Attr) error {
	fi, err := n.root.underlying.Stat(n.path)
	if err != nil {
		return convertError(err)
	}
	fileInfoToAttr(fi, attr)
	return nil
}

func fileInfoToAttr(fi os.FileInfo, out *fuse.Attr) {
	out.Mode = fi.Mode()
	out.Size = uint64(fi.Size())
	out.Mtime = fi.ModTime()
}

func (n *node) Lookup(ctx context.Context, req *fuse.LookupRequest, resp *fuse.LookupResponse) (fs.Node, error) {
	if err := n.root.callHook(ctx, req); err != nil {
		return nil, convertError(err)
	}
	return &node{n.root, path.Join(n.path, req.Name)}, nil
}

func (n *node) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {
	if err := n.root.callHook(ctx, req); err != nil {
		return nil, convertError(err)
	}
	if dfs, ok := n.root.underlying.(billy.Dir); ok {
		fn := path.Join(n.path, req.Name)
		if err := dfs.MkdirAll(fn, os.FileMode(req.Mode)); err != nil {
			return nil, convertError(err)
		}
		return &node{n.root, fn}, nil
	}
	return nil, fuse.ENOSYS
}

// Unlink removes a file.
func (n *node) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	if err := n.root.callHook(ctx, req); err != nil {
		return convertError(err)
	}
	return convertError(n.root.underlying.Remove(path.Join(n.path, req.Name)))
}

// Symlink creates a symbolic link.
func (n *node) Symlink(ctx context.Context, req *fuse.SymlinkRequest) (fs.Node, error) {
	if err := n.root.callHook(ctx, req); err != nil {
		return nil, convertError(err)
	}
	if sfs, ok := n.root.underlying.(billy.Symlink); ok {
		fn := path.Join(n.path, req.NewName)
		if err := sfs.Symlink(req.Target, fn); err != nil {
			return nil, convertError(err)
		}
		return &node{n.root, fn}, nil
	}
	return nil, fuse.ENOSYS
}

// Readlink reads the target of a symbolic link.
func (n *node) Readlink(ctx context.Context, req *fuse.ReadlinkRequest) (string, error) {
	if err := n.root.callHook(ctx, req); err != nil {
		return "", convertError(err)
	}
	if sfs, ok := n.root.underlying.(billy.Symlink); ok {
		fn, err := sfs.Readlink(n.path)
		if err != nil {
			return "", convertError(err)
		}
		return fn, nil
	}
	return "", fuse.ENOSYS
}

// Rename renames a file.
func (n *node) Rename(ctx context.Context, req *fuse.RenameRequest, newDir fs.Node) error {
	if err := n.root.callHook(ctx, req); err != nil {
		return convertError(err)
	}
	return convertError(n.root.underlying.Rename(path.Join(n.path, req.OldName), path.Join(newDir.(*node).path, req.NewName)))
}

func (n *node) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	if err := n.root.callHook(ctx, req); err != nil {
		return convertError(err)
	}
	if req.Valid.AtimeNow() {
		req.Valid |= fuse.SetattrAtime
		req.Atime = time.Now()
	}
	if req.Valid.MtimeNow() {
		req.Valid |= fuse.SetattrMtime
		req.Atime = time.Now()
	}
	if req.Valid.Mode() || req.Valid.Uid() || req.Valid.Gid() || req.Valid.Atime() || req.Valid.Mtime() {
		cfs, ok := n.root.underlying.(billy.Change)
		if !ok {
			return fuse.ENOTSUP
		}
		if req.Valid.Mode() {
			if err := cfs.Chmod(n.path, req.Mode); err != nil {
				return convertError(err)
			}
		}
		if req.Valid.Uid() || req.Valid.Gid() {
			uid := int(req.Uid)
			if !req.Valid.Uid() {
				uid = -1
			}
			gid := int(req.Gid)
			if !req.Valid.Gid() {
				gid = -1
			}
			if err := cfs.Lchown(n.path, uid, gid); err != nil {
				return convertError(err)
			}
		}
		if req.Valid.Atime() || req.Valid.Mtime() {
			// TODO: Handle correctly.
			if req.Valid.Mtime() {
				if err := cfs.Chtimes(n.path, req.Atime, req.Mtime); err != nil {
					return convertError(err)
				}
			}
		}
	}
	if req.Valid.Size() {
		fh, err := n.root.underlying.OpenFile(n.path, os.O_WRONLY, 0777)
		if err != nil {
			return convertError(err)
		}
		defer fh.Close()
		if err := fh.Truncate(int64(req.Size)); err != nil {
			return convertError(err)
		}
	}
	// TODO: if req.Valid.Handle()
	// TODO: if req.Valid.LockOwner()
	return nil
}

func (n *node) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	if err := n.root.callHook(ctx, req); err != nil {
		return nil, nil, convertError(err)
	}
	fn := path.Join(n.path, req.Name)
	fh, err := n.root.underlying.OpenFile(fn, int(req.Flags), req.Mode)
	if err != nil {
		return nil, nil, convertError(err)
	}
	return &node{n.root, fn}, &handle{root: n.root, fh: fh}, nil
}

func (n *node) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	if err := n.root.callHook(ctx, req); err != nil {
		return nil, convertError(err)
	}
	if req.Dir {
		return &dirHandle{root: n.root, path: n.path}, nil
	}
	fh, err := n.root.underlying.OpenFile(n.path, int(req.Flags), 0777)
	if err != nil {
		return nil, convertError(err)
	}
	return &handle{root: n.root, fh: fh}, nil
}

type handle struct {
	root      *root
	fh        billy.File
	writeLock sync.Mutex
}

var _ fs.HandleReader = &handle{}
var _ fs.HandleReleaser = &handle{}
var _ fs.HandleWriter = &handle{}

func (h *handle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	if err := h.root.callHook(ctx, req); err != nil {
		return convertError(err)
	}
	resp.Data = make([]byte, req.Size)
	n, err := h.fh.ReadAt(resp.Data, req.Offset)
	if err == io.EOF {
		err = nil
	}
	resp.Data = resp.Data[:n]
	return convertError(err)
}

func (h *handle) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	if err := h.root.callHook(ctx, req); err != nil {
		return convertError(err)
	}
	if wa, ok := h.fh.(io.WriterAt); ok {
		n, err := wa.WriteAt(req.Data, req.Offset)
		if err != nil {
			return convertError(err)
		}
		resp.Size = n
		return nil
	}
	h.writeLock.Lock()
	defer h.writeLock.Unlock()
	if _, err := h.fh.Seek(req.Offset, io.SeekStart); err != nil {
		return convertError(err)
	}
	n, err := h.fh.Write(req.Data)
	if err != nil {
		return convertError(err)
	}
	resp.Size = n
	return nil
}

func (h *handle) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	if err := h.root.callHook(ctx, req); err != nil {
		return convertError(err)
	}
	return convertError(h.fh.Close())
}

type dirHandle struct {
	root *root
	path string
}

var _ fs.HandleReadDirAller = &dirHandle{}

func (h *dirHandle) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	if dfs, ok := h.root.underlying.(billy.Dir); ok {
		entries, err := dfs.ReadDir(h.path)
		if err != nil {
			return nil, convertError(err)
		}
		ret := make([]fuse.Dirent, len(entries))
		for i, e := range entries {
			t := fuse.DT_File
			if e.IsDir() {
				t = fuse.DT_Dir
			} else if e.Mode()&os.ModeSymlink > 0 {
				t = fuse.DT_Link
			}
			ret[i] = fuse.Dirent{
				Name: e.Name(),
				Type: t,
			}
		}
		return ret, nil
	}
	return nil, fuse.ENOSYS
}

func convertError(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := err.(fuse.ErrorNumber); ok {
		return err
	}
	if os.IsExist(err) {
		return fuse.EEXIST
	}
	if os.IsNotExist(err) {
		return fuse.ENOENT
	}
	if os.IsPermission(err) {
		return fuse.EPERM
	}
	if errors.Is(err, os.ErrInvalid) || errors.Is(err, os.ErrClosed) || errors.Is(err, billy.ErrCrossedBoundary) {
		return fuse.Errno(syscall.EINVAL)
	}
	if errors.Is(err, billy.ErrNotSupported) {
		return fuse.ENOTSUP
	}
	return fuse.EIO
}
