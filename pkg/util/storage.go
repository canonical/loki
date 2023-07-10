package util

import "syscall"

// DiskStatus contains disk usage for a given path/disk
type DiskStatus struct {
	All         uint64  `json:"All"`
	Used        uint64  `json:"Used"`
	Free        uint64  `json:"Free"`
	UsedPercent float64 `json:"UsedPercent"`
}

// DiskUsage gets the disk usage of path/disk
func DiskUsage(path string) (DiskStatus, error) {
	fs := syscall.Statfs_t{}
	disk := DiskStatus{}
	if err := syscall.Statfs(path, &fs); err != nil {
		return disk, err
	}

	disk.All = fs.Blocks * uint64(fs.Bsize)
	disk.Free = fs.Bfree * uint64(fs.Bsize)
	disk.Used = disk.All - disk.Free
	disk.UsedPercent = (float64(disk.Used) / float64(disk.All)) * float64(100)
	return disk, nil
}
