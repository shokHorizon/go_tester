package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/shokHorizon/go_tester/internals/entity"
	"github.com/shokHorizon/go_tester/internals/repos"
)

const (
	kafkaBroker   = "localhost:9092"
	kafkaTopic    = "tasks"
	kafkaConsumer = "task-consumer"
	redisAddress  = "localhost:9099"
	redisPassword = "1234lolkek"
	redisDB       = 0
	testsPath     = "/kursik/tests"
	executePath   = "/kursik/exec"
	maxSolutions  = 10
	maxTraces     = 3
)

func main() {
	// Create a Kafka repository to read tasks from Kafka.
	kafkaRepo, err := repos.NewKafkaRepository([]string{kafkaBroker}, kafkaTopic, kafkaConsumer)
	if err != nil {
		fmt.Printf("Error creating Kafka repository: %v\n", err)
		os.Exit(1)
	}

	redisRepo, err := repos.NewRedisTracebackRepository(redisAddress, redisPassword, redisDB)
	if err != nil {
		fmt.Printf("Error creating Redis repository: %v\n", err)
		os.Exit(1)
	}

	// Set up a context that can be cancelled.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Set up a wait group to wait for all goroutines to finish.
	var wg sync.WaitGroup

	// Set up a buffered channel to limit the number of solutions that can be processed concurrently.
	solutionCh := make(chan entity.Solution, maxSolutions)
	// Set up a buffered channel to limit the number of traces that can be processed concurrently.
	traceCh := make(chan entity.Solution, maxTraces)

	// Set up signal handling to cancel the context when a signal is received.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Println("Received interrupt signal, cancelling context...")
		cancel()
	}()

	// Start a fixed number of worker goroutines to process solutions from the channel.
	for i := 0; i < maxSolutions; i++ {
		go func() {
			for solution := range solutionCh {
				wg.Add(1)
				go func(s entity.Solution) {
					defer wg.Done()
					err := executeSolution(ctx, s, testsPath, executePath, 10*time.Second)
					if err != nil {
						fmt.Printf("Error executing solution: %v\n", err)
						s.Code = err.Error()
						traceCh <- s
					}
				}(solution)
			}
		}()
	}

	for i := 0; i < maxTraces; i++ {
		go func() {
			for trace := range traceCh {
				wg.Add(1)
				go func(t entity.Solution) {
					defer wg.Done()
					err := redisRepo.SaveTraceback(t.TaskID, t.UserID, t.Code)
					if err != nil {
						fmt.Printf("Error writing solution: %v\n", err)
					}
				}(trace)
			}
		}()
	}

	// Read solutions from Kafka and send them to the channel for processing.
	for done := false; !done; {
		select {
		case <-ctx.Done():
			// Context cancelled, stop reading solutions.
			fmt.Println("Context cancelled, stopping solution processing...")
			close(solutionCh)
			close(traceCh)
			done = true
		default:
			solution, err := kafkaRepo.ReadSolution(ctx)
			if err != nil {
				fmt.Printf("Error reading solution: %v\n", err)
				done = true
			}
			fmt.Printf("Received solution: %+v\n", solution)
			solutionCh <- *solution
		}
	}

	// Wait for all goroutines to finish.
	wg.Wait()
}

func executeSolution(ctx context.Context, s entity.Solution, executePath, testsPath string, timeOut time.Duration) error {
	fmt.Printf("Executing solution %d for user %d...\n", s.TaskID, s.UserID)

	// Create a temporary directory to hold the solution and test files
	tempDir, err := os.MkdirTemp(executePath, fmt.Sprintf("%d-%d", s.TaskID, s.UserID))
	if err != nil {
		fmt.Printf("Error creating temp dir: %v\n", err)
		return err
	}
	defer os.RemoveAll(tempDir)

	// Copy the solution and test files to the temp dir
	solutionFile := filepath.Join(tempDir, "solution.go")
	newTestFile := filepath.Join(tempDir, "test.go")
	testFile := filepath.Join(testsPath, fmt.Sprint(s.TaskID), "main_test.go")

	tests, err := os.ReadFile(testFile)
	if err != nil {
		fmt.Printf("Error reading tests file: %v\n", err)
		return err
	}
	err = os.WriteFile(solutionFile, []byte(s.Code), 0644)
	if err != nil {
		fmt.Printf("Error writing solution file: %v\n", err)
		return err
	}
	err = os.WriteFile(newTestFile, tests, 0644)
	if err != nil {
		fmt.Printf("Error writing test file: %v\n", err)
		return err
	}

	// Create a new context with the specified timeout
	timeoutCtx, cancel := context.WithTimeout(ctx, timeOut)
	defer cancel()

	// Run the tests inside a chroot
	cmd := exec.CommandContext(timeoutCtx, "chroot", tempDir, "go", "test")
	traces := bytes.NewBuffer(make([]byte, 256))
	cmd.Stdout = os.Stdout
	cmd.Stderr = traces
	err = cmd.Run()
	if err != nil {
		fmt.Printf("Error running tests: %v\n", err)
		return fmt.Errorf("error occured: %v", traces.String())
	}
	return nil
}
