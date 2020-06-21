package service

import (
	"fmt"
	"path/filepath"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/golang/glog"
	"github.com/uwedeportivo/commander"
	"github.com/uwedeportivo/romba/worker"
)

type bloomWorker struct {
	pm  *bloomGru
	idx int
}

func (pw *bloomWorker) Process(path string, _ int64) error {
	pw.pm.rs.depot.PopulateBloom(path)
	return nil
}

func (pw *bloomWorker) Close() error {
	return nil
}

type bloomGru struct {
	rs            *RombaService
	numWorkers    int
	numSubWorkers int
	pt            worker.ProgressTracker
}

func (pm *bloomGru) CalculateWork() bool {
	return true
}

func (pm *bloomGru) Accept(path string) bool {
	ext := filepath.Ext(path)
	return ext == ".gz"
}

func (pm *bloomGru) NewWorker(workerIndex int) worker.Worker {
	return &bloomWorker{
		pm:  pm,
		idx: workerIndex,
	}
}

func (pm *bloomGru) NumWorkers() int {
	return pm.numWorkers
}

func (pm *bloomGru) ProgressTracker() worker.ProgressTracker {
	return pm.pt
}

func (pm *bloomGru) FinishUp() error {
	return nil
}

func (pm *bloomGru) Start() error {
	return nil
}

func (pm *bloomGru) Scanned(_ int, _ int64, _ string) {
}

func (rs *RombaService) popBloom(cmd *commander.Command, _ []string) error {
	rs.jobMutex.Lock()
	defer rs.jobMutex.Unlock()

	if rs.busy {
		p := rs.pt.GetProgress()

		_, err := fmt.Fprintf(cmd.Stdout, "still busy with %s: (%d of %d files) and (%s of %s) \n", rs.jobName,
			p.FilesSoFar, p.TotalFiles, humanize.IBytes(uint64(p.BytesSoFar)), humanize.IBytes(uint64(p.TotalBytes)))
		return err
	}

	numWorkers := cmd.Flag.Lookup("workers").Value.Get().(int)
	numSubWorkers := cmd.Flag.Lookup("subworkers").Value.Get().(int)

	rs.pt.Reset()
	rs.busy = true
	rs.jobName = "popBloom"

	go func() {
		ticker := time.NewTicker(time.Second * 5)
		stopTicker := make(chan bool)
		go func() {
			glog.Infof("starting progress broadcaster")
			for {
				select {
				case t := <-ticker.C:
					rs.broadCastProgress(t, false, false, "", nil)
				case <-stopTicker:
					glog.Info("stopped progress broadcaster")
					return
				}
			}
		}()

		var endMsg string

		err := rs.depot.ClearBloomFilters()
		if err != nil {
			glog.Errorf("error clearing bloom: %v", err)
		} else {
			pm := &bloomGru{
				rs:            rs,
				numWorkers:    numWorkers,
				numSubWorkers: numSubWorkers,
				pt:            rs.pt,
			}

			rps, err := rs.depot.ResumePopBloomPaths()
			if err != nil {
				glog.Errorf("error finding resume points for populating bloom: %v", err)
			} else {
				endMsg, err = worker.ResumeWork("populating bloom", rps, pm)
				if err != nil {
					glog.Errorf("error populating bloom: %v", err)
				}
			}
		}

		ticker.Stop()
		stopTicker <- true

		rs.jobMutex.Lock()
		rs.busy = false
		rs.jobName = ""
		rs.jobMutex.Unlock()

		rs.broadCastProgress(time.Now(), false, true, endMsg, err)
		glog.Infof("service finished populating bloom")
	}()

	glog.Infof("service starting popBloom")
	_, err := fmt.Fprintf(cmd.Stdout, "started popBloom")
	return err
}
