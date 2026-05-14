package file

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/multiversx/mx-chain-core-go/core"
	logger "github.com/multiversx/mx-chain-logger-go"
	"github.com/multiversx/mx-chain-logger-go/redirects"
)

const (
	defaultFileLifeSpan     = time.Hour * 24
	defaultFileSizeInMB     = 1024 // 1GB
	recheckFileSizeInterval = time.Second * 30
	oneMegaByte             = 1024 * 1024
	minFileLifeSpan         = time.Second
	minSizeInMB             = uint64(1)
	maxSizeInMB             = uint64(1024 * 1024) // 1TB
)

var log = logger.GetOrCreate("common/logging")
var trueCheckHandler = func() bool { return true }

type logLifeSpanner interface {
	resetDuration(newDuration time.Duration)
	reset()
	close()
}

// fileLogging is able to rotate the log files
type fileLogging struct {
	mutOperation            sync.RWMutex
	currentFile             *os.File
	workingDir              string
	defaultLogsPath         string
	logFilePrefix           string
	cancelFunc              func()
	mutIsClosed             sync.Mutex
	lifeSpanSize            uint64
	isClosed                bool
	timeBasedLogLifeSpanner logLifeSpanner
	sizeBaseLogLifeSpanner  logLifeSpanner
	notifyChan              chan struct{}
}

// ArgsFileLogging is the argument for the file logger
type ArgsFileLogging struct {
	WorkingDir      string
	DefaultLogsPath string
	LogFilePrefix   string
}

// NewFileLogging creates a file log watcher used to break the log file into multiple smaller files
func NewFileLogging(args ArgsFileLogging) (*fileLogging, error) {
	fl := &fileLogging{
		workingDir:      args.WorkingDir,
		defaultLogsPath: args.DefaultLogsPath,
		logFilePrefix:   args.LogFilePrefix,
		isClosed:        false,
		lifeSpanSize:    defaultFileSizeInMB * oneMegaByte,
		notifyChan:      make(chan struct{}),
	}

	fl.timeBasedLogLifeSpanner = newLifeSpanner(fl.notifyChan, trueCheckHandler, defaultFileLifeSpan)
	fl.sizeBaseLogLifeSpanner = newLifeSpanner(fl.notifyChan, fl.sizeReached, recheckFileSizeInterval)

	fl.recreateLogFile()

	// we need this function as to call file.Close() when the code panics and the deferred function associated
	// with the file pointer in the main func will never be reached
	//
	// ISSUE-049: currentFile may legitimately be nil here (recreateLogFile
	// above logs and returns early if file creation or observer
	// registration fails). Without this guard the finalizer would panic on
	// nil-dereference, which is worst-possible behaviour for a process-exit
	// path. Nil-check, then close.
	runtime.SetFinalizer(fl, func(fileLogHandler *fileLogging) {
		if fileLogHandler.currentFile != nil {
			_ = fileLogHandler.currentFile.Close()
		}
	})

	ctx, cancelFunc := context.WithCancel(context.Background())
	go fl.autoRecreateFile(ctx)
	fl.cancelFunc = cancelFunc

	return fl, nil
}

func (fl *fileLogging) createFile() (*os.File, error) {
	logDirectory := filepath.Join(fl.workingDir, fl.defaultLogsPath)

	return core.CreateFile(
		core.ArgCreateFileArgument{
			Prefix:        fl.logFilePrefix,
			Directory:     logDirectory,
			FileExtension: "log",
		},
	)
}

func (fl *fileLogging) recreateLogFile() {
	newFile, err := fl.createFile()
	if err != nil {
		log.Error("error creating new log file", "error", err)
		return
	}

	fl.mutOperation.Lock()
	defer fl.mutOperation.Unlock()

	oldFile := fl.currentFile
	err = logger.AddLogObserver(newFile, &logger.PlainFormatter{})
	if err != nil {
		// ISSUE-050: previously this returned without closing newFile,
		// leaking the underlying file descriptor on every rotation
		// attempt that failed observer registration. On a long-running
		// node with one rotation per day, this is one FD per day until
		// the process hits its rlimit. Close newFile before returning;
		// the existing oldFile (if any) stays in place via the
		// no-state-mutation path below.
		closeErr := newFile.Close()
		log.LogIfError(closeErr, "step", "closing new file after AddLogObserver failed")
		log.Error("error adding log observer", "error", err)
		return
	}

	errNotCritical := redirects.RedirectStderr(newFile)
	log.LogIfError(errNotCritical, "step", "redirecting std error")

	fl.currentFile = newFile

	if oldFile == nil {
		return
	}

	errNotCritical = oldFile.Close()
	log.LogIfError(errNotCritical, "step", "closing old log file")

	errNotCritical = logger.RemoveLogObserver(oldFile)
	log.LogIfError(errNotCritical, "step", "removing old log observer")

	fl.timeBasedLogLifeSpanner.reset()
}

func (fl *fileLogging) autoRecreateFile(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			log.Debug("closing fileLogging.autoRecreateFile go routine")
			return
		case <-fl.notifyChan:
			fl.recreateLogFile()
		}
	}
}

// ChangeFileLifeSpan changes the log file span
func (fl *fileLogging) ChangeFileLifeSpan(newDuration time.Duration, sizeInMB uint64) error {
	err := checkArgs(newDuration, sizeInMB)
	if err != nil {
		return err
	}

	fl.mutIsClosed.Lock()
	isClosed := fl.isClosed
	fl.mutIsClosed.Unlock()

	if isClosed {
		return core.ErrFileLoggingProcessIsClosed
	}

	size := sizeInMB * oneMegaByte
	fl.mutOperation.Lock()
	fl.lifeSpanSize = size
	fl.timeBasedLogLifeSpanner.resetDuration(newDuration)
	fl.mutOperation.Unlock()

	log.Debug("changed the log life span", "new duration", newDuration, "new size", core.ConvertBytes(size))

	return nil
}

func checkArgs(lifeSpanDuration time.Duration, lifeSpanInMB uint64) error {
	if lifeSpanDuration < minFileLifeSpan {
		return fmt.Errorf("%w for the life span duration, minimum: %v, provided: %v",
			errInvalidParameter, minFileLifeSpan, lifeSpanDuration)
	}
	if lifeSpanInMB < minSizeInMB {
		return fmt.Errorf("%w for the life span in MB, minimum: %v, provided: %v",
			errInvalidParameter, minSizeInMB, lifeSpanInMB)
	}
	if lifeSpanInMB > maxSizeInMB {
		return fmt.Errorf("%w for the life span in MB, maximum: %v, provided: %v",
			errInvalidParameter, maxSizeInMB, lifeSpanInMB)
	}

	return nil
}

func (fl *fileLogging) sizeReached() bool {
	fl.mutOperation.RLock()
	currentFile := fl.currentFile
	fl.mutOperation.RUnlock()

	if currentFile == nil {
		return false
	}

	stats, errNotCritical := currentFile.Stat()
	if errNotCritical != nil {
		log.Warn("error retrieving log statistics", "error", errNotCritical)
		return false
	}

	return stats.Size() >= int64(fl.lifeSpanSize)
}

// Close closes the file logging handler
func (fl *fileLogging) Close() error {
	fl.mutIsClosed.Lock()
	if fl.isClosed {
		fl.mutIsClosed.Unlock()
		return nil
	}

	fl.isClosed = true
	fl.mutIsClosed.Unlock()

	// ISSUE-049: currentFile may legitimately be nil if every prior
	// recreateLogFile() attempt failed (createFile error or
	// AddLogObserver error short-circuits without ever assigning
	// fl.currentFile). Closing an already-nil-recovered Close() must
	// still tear down the spanners and cancelFunc, but must not
	// nil-dereference on the file pointer.
	fl.mutOperation.Lock()
	var err error
	if fl.currentFile != nil {
		err = fl.currentFile.Close()
	}
	fl.mutOperation.Unlock()

	fl.cancelFunc()
	fl.sizeBaseLogLifeSpanner.close()
	fl.timeBasedLogLifeSpanner.close()

	return err
}

// IsInterfaceNil returns true if there is no value under the interface
func (fl *fileLogging) IsInterfaceNil() bool {
	return fl == nil
}
