package retention

import (
	"context"
	"fmt"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/services"
	"github.com/grafana/loki/pkg/storage/chunk/client/local"
	"github.com/grafana/loki/pkg/util"
	util_log "github.com/grafana/loki/pkg/util/log"
	"sync"
	"time"
)

const defaultInterval = 60 * time.Second

type CompactionFn func(context.Context, int, string) error

func NewSizeBasedRetentionService(logger log.Logger, threshold int, fsconfig local.FSConfig, compactionFn CompactionFn) *SizeBasedRetentionService {
	svc := &SizeBasedRetentionService{
		logger:           logger,
		Interval:         defaultInterval,
		Threshold:        threshold,
		WorkingDirectory: fsconfig.Directory,
		CompactionFn:     compactionFn,
	}
	svc.Service = services.NewTimerService(
		svc.Interval, svc.starting, svc.loop, svc.stopping,
	).WithName("size-based retention cleaner")

	return svc
}

type SizeBasedRetentionService struct {
	services.Service
	logger           log.Logger
	mtx              sync.RWMutex
	Interval         time.Duration
	Threshold        int
	WorkingDirectory string
	CompactionFn     CompactionFn
}

func (svc *SizeBasedRetentionService) starting(ctx context.Context) error {
	_ = level.Info(svc.logger).Log("msg", "size-based retention service is starting")
	return nil
}

func (svc *SizeBasedRetentionService) loop(ctx context.Context) error {
	svc.mtx.Lock()
	defer svc.mtx.Unlock()

	var exceeded bool
	var err error

	if exceeded, err = svc.ThresholdExceeded(); err != nil {
		_ = level.Info(svc.logger).Log("msg", "size-based compaction failed", "err", err)
		return err
	}

	if !exceeded {
		return nil
	}

	_ = level.Info(svc.logger).Log("msg", "running size-based compaction")
	if err = svc.CompactionFn(ctx, svc.Threshold, svc.WorkingDirectory); err != nil {
		return err
	}

	return nil
}

func (svc *SizeBasedRetentionService) stopping(stoppingError error) error {
	_ = level.Info(svc.logger).Log("msg", "size-based retention service is stopping")
	return nil
}

func (svc *SizeBasedRetentionService) ThresholdExceeded() (bool, error) {
	diskUsage, err := svc.getDiskUsage()
	if err != nil {
		return false, err
	}
	if diskUsage.UsedPercent < float64(svc.Threshold) {
		_ = level.Debug(util_log.Logger).Log("msg", "Disk usage below threshold, skipping compaction.")
		return false, nil
	}
	_ = level.Info(util_log.Logger).Log("msg", "Disk usage threshold exceeded, running compaction.")
	return true, nil
}

func (svc *SizeBasedRetentionService) getDiskUsage() (util.DiskStatus, error) {
	usage, err := util.DiskUsage(svc.WorkingDirectory)
	if err != nil {
		return util.DiskStatus{}, err
	}
	_ = level.Info(util_log.Logger).Log(
		"msg",
		"Detected disk usage percentage",
		"usage",
		fmt.Sprintf("%.2f%%", usage.UsedPercent))
	return usage, nil
}

type SizeBasedRetentionCleaner struct{}

func NewSizeBasedRetentionCleaner() *SizeBasedRetentionCleaner {
	return &SizeBasedRetentionCleaner{}
}
