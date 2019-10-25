package lsf

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/drone/drone-runtime/engine"
	"github.com/hpcloud/tail"
	"github.com/phayes/freeport"
)

type lsfEngine struct {
	home string
}

type errorString struct {
	s string
}

func (e *errorString) Error() string {
	return e.s
}

// New returns a new LSF controller engine
func New(home string) engine.Engine {
	return &lsfEngine{
		home: home,
	}
}

func (e *lsfEngine) Setup(ctx context.Context, spec *engine.Spec) error {
	workdir, err := ioutil.TempDir(e.home, "lsf")
	if err != nil {
		log.Fatalln("Unable to create working directory")
		return err
	}
	spec.Lsf = &engine.LsfConfig{
		Workdir: workdir,
	}
	return nil
}

func (e *lsfEngine) Create(ctx context.Context, spec *engine.Spec, step *engine.Step) error {
	log.Println(step.Metadata.Name)
	f, err := ioutil.TempFile(spec.Lsf.Workdir, "lsf*.sh")
	if err != nil {
		return err
	}
	step.Lsf = &engine.LsfStep{
		Shell:     f,
		ShellName: f.Name(),
		JobName: fmt.Sprintf(
			"%s_%s:%s_%s:%s_%s",
			step.Metadata.Labels["io.drone.repo.namespace"],
			step.Metadata.Labels["io.drone.repo.name"],
			step.Metadata.Labels["io.drone.stage.name"],
			step.Metadata.Labels["io.drone.stage.number"],
			step.Metadata.Name,
			step.Metadata.Labels["io.drone.build.number"],
		),
		Running: false,
	}
	ShellType, ok := step.Envs["SHELL_TYPE"]
	if ok {
		step.Lsf.ShellType = ShellType
	} else {
		step.Lsf.ShellType = "csh"
	}

	if step.Metadata.Name == "clone" {
		e.toLsfClone(spec, step)
		step.WorkingDir = spec.Lsf.Workdir
	} else {
		for k, v := range step.Envs {
			v = strings.Trim(v, "\n")
			v = envEscape(v)
			if step.Lsf.ShellType == "csh" {
				v = strings.ReplaceAll(v, "\n", "\\n")
				step.Lsf.Shell.WriteString(fmt.Sprintf("setenv %s '%s'\n", k, v))
			} else {
				step.Lsf.Shell.WriteString(fmt.Sprintf("export %s='%s'\n", k, v))
			}
		}
		for _, sec := range step.Secrets {
			secret, ok := engine.LookupSecret(spec, sec)
			if ok {
				if step.Lsf.ShellType == "csh" {
					step.Lsf.Shell.WriteString(
						fmt.Sprintf("setenv %s '%s'\n", sec.Env, envEscape(secret.Data)))
				} else {
					step.Lsf.Shell.WriteString(
						fmt.Sprintf("export %s='%s'\n", sec.Env, envEscape(secret.Data)))
				}
			}
		}
		for _, mount := range step.Files {
			file, ok := engine.LookupFile(spec, mount.Name)
			if !ok {
				continue
			}
			content := string(file.Data)
			lines := strings.Split(content, "\n")
			content = strings.Join(lines[15:], "\n")
			if step.Lsf.ShellType == "csh" {
				content = strings.ReplaceAll(content, "\\\"", "\"'\"'\"")
			}
			step.Lsf.Shell.Write([]byte(content))
		}
		step.Lsf.Shell.Close()
		step.WorkingDir = path.Join(spec.Lsf.Workdir, "src")
	}
	return nil
}

func (e *lsfEngine) Start(ctx context.Context, spec *engine.Spec, step *engine.Step) error {
	var err error
	var port int
	var rd *io.PipeReader
	rd, step.Lsf.NetPipe = io.Pipe()

	step.Lsf.Running = true
	localFifo := step.Lsf.ShellName + ".local"
	mkfifo := exec.Command("mkfifo", localFifo)
	if err = mkfifo.Start(); err != nil {
		return err
	}
	mkfifo.Wait()
	if port, err = freeport.GetFreePort(); err != nil {
		log.Println(err)
		return err
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		step.Lsf.TailFile, _ = tail.TailFile(localFifo, tail.Config{
			Follow: true,
			Poll:   true,
			Pipe:   true,
		})
	}()
	go func() {
		defer wg.Done()
		port, err = startNetCat(step)
		log.Printf("NetCat Port is %d", port)
	}()
	wg.Wait()
	if err != nil {
		return err
	}
	step.Lsf.WaitGroup.Go(func() error {
		file, err := os.OpenFile(localFifo, os.O_RDWR, 0777)
		if err != nil {
			return err
		}
		io.Copy(file, rd)
		file.Close()
		rd.Close()
		return nil
	})
	step.Lsf.Job = exec.Command(
		"sh", "-c", fmt.Sprintf("%s '%s'",
			bsubCmd(spec, step),
			shellCmd(port, step),
		))
	if err = step.Lsf.Job.Start(); err != nil {
		log.Printf("Bsub Error... %s", err)
		return err
	}
	return nil
}

func (e *lsfEngine) Wait(ctx context.Context, spec *engine.Spec, step *engine.Step) (*engine.State, error) {
	exitCode := 0
	err := step.Lsf.Job.Wait()
	step.Lsf.NetPipe.Close()
	step.Lsf.NetCat.Wait()
	step.Lsf.Running = false
	if err != nil {
		if exitError, ok := err.(*exec.ExitError); ok {
			exitCode = exitError.ExitCode()
		} else {
			return nil, err
		}
	}
	return &engine.State{
		Exited:    true,
		ExitCode:  exitCode,
		OOMKilled: false,
	}, nil
}

func (e *lsfEngine) Tail(ctx context.Context, spec *engine.Spec, step *engine.Step) (io.ReadCloser, error) {
	var fout, ferr string
	fout = step.Lsf.Shell.Name() + ".out"
	ferr = step.Lsf.Shell.Name() + ".err"
	rc, wc := io.Pipe()
	step.Lsf.WaitGroup.Go(func() error {
		stdout, _ := copyTail(step)
		io.Copy(wc, stdout)
		waitLsfLog(step)
		if fileExist(fout) {
			stdout, _ := os.Open(fout)
			io.Copy(wc, stdout)
			stdout.Close()
		}
		if fileExist(ferr) {
			stderr, _ := os.Open(ferr)
			io.Copy(wc, stderr)
			stderr.Close()
		}
		wc.Close()
		rc.Close()
		return nil
	})
	return rc, nil
}

func (e *lsfEngine) Destroy(ctx context.Context, spec *engine.Spec) error {
	log.Printf("Destory %s", spec.Lsf.Workdir)
	for _, step := range spec.Steps {
		if step.Lsf == nil {
			continue
		}
		log.Printf("Terminate %s", step.Lsf.JobName)
		cmd := exec.Command("bkill", "-J", step.Lsf.JobName)
		cmd.Start()
		cmd.Wait()
		step.Lsf.Running = false
		if err := step.Lsf.NetCat.Process.Kill(); err != nil {
			log.Println("failed to kill process: ", err)
		}
		step.Lsf.NetPipe.Close()
		step.Lsf.WaitGroup.Wait()
		step.Lsf.NetCat.Wait()
		// timeOutWait(step.Lsf.NetCat)
	}
	log.Println("Remove" + spec.Lsf.Workdir)
	// var err error
	// retry := 0
	// err = os.RemoveAll(spec.Lsf.Workdir)
	// for err != nil && retry < 10 {
	// 	time.Sleep(100 * time.Millisecond)
	// 	err = os.RemoveAll(spec.Lsf.Workdir)
	// 	retry++
	// }
	return nil
}

// ClearDir remove all files in directory
func ClearDir(dir string) error {
	log.Println("Destroy")
	files, err := filepath.Glob(filepath.Join(dir, "*"))
	if err != nil {
		return err
	}
	for _, file := range files {
		err = os.RemoveAll(file)
		if err != nil {
			log.Fatalln(err)
			return err
		}
	}
	return nil
}

func gitRepos(step *engine.Step) string {
	var repos string
	_, ok := step.Envs["DRONE_NETRC_USERNAME"]
	if ok {
		unpak := strings.SplitN(step.Envs["DRONE_GIT_HTTP_URL"], ":", 2)
		proto := unpak[0]
		repos = strings.Trim(unpak[1], "/")
		repos = fmt.Sprintf(
			"%s://%s:%s@%s", proto,
			step.Envs["DRONE_NETRC_USERNAME"],
			step.Envs["DRONE_NETRC_PASSWORD"],
			repos)
	} else {
		repos = step.Envs["DRONE_GIT_HTTP_URL"]
	}
	return repos
}

func (e *lsfEngine) toLsfClone(spec *engine.Spec, step *engine.Step) error {
	repos := gitRepos(step)
	log.Println(repos)
	target := path.Join(spec.Lsf.Workdir, "src")
	step.Lsf.Shell.WriteString(fmt.Sprintf("git clone %s %s\n", repos, target))
	step.Lsf.Shell.WriteString(fmt.Sprintf("cd %s\n", target))
	step.Lsf.Shell.WriteString(
		fmt.Sprintf("git checkout %s\n", step.Envs["DRONE_COMMIT_SHA"]))
	step.Lsf.Shell.Close()
	return nil
}

func bsubCmd(spec *engine.Spec, step *engine.Step) string {
	cmd := fmt.Sprintf("bsub -K -cwd %s -J %s -o %s -e %s",
		step.WorkingDir, step.Lsf.JobName,
		step.Lsf.ShellName+".out",
		step.Lsf.ShellName+".err",
	)
	option, ok := step.Envs["BSUB_OPTION"]
	if ok {
		cmd = cmd + " " + option
	}
	return cmd
}

func shellCmd(port int, step *engine.Step) string {
	ShellOption, ok := step.Envs["SHELL_OPTION"]
	if !ok {
		if step.Lsf.ShellType == "csh" {
			ShellOption = "-f -e"
		} else {
			ShellOption = "-e"
		}
	}
	host, _ := os.Hostname()
	cmd := fmt.Sprintf(
		"mkfifo %s && sh -c \"nc %s %d < %s &\"; %s %s %s >& %s",
		step.Lsf.ShellName+".remote",
		host, port,
		step.Lsf.ShellName+".remote",
		step.Lsf.ShellType,
		ShellOption,
		step.Lsf.ShellName,
		step.Lsf.ShellName+".remote",
	)
	return cmd
}

func fileExist(path string) bool {
	_, err := os.Stat(path)
	if err == nil {
		return true
	}
	return false
}

// copyTail implements tail -f to the LSF log
func copyTail(step *engine.Step) (io.ReadCloser, error) {
	tout := step.Lsf.TailFile
	rc, wc := io.Pipe()
	step.Lsf.WaitGroup.Go(func() error {
		for step.Lsf.Running {
			time.Sleep(100 * time.Millisecond)
		}
		tout.StopAtEOF()
		return nil
	})
	step.Lsf.WaitGroup.Go(func() error {
		for line := range tout.Lines {
			wc.Write([]byte(line.Text))
		}
		wc.Write([]byte("\n[End of Step Output]\n"))
		wc.Close()
		rc.Close()
		return nil
	})
	return rc, nil
}

func envEscape(s string) string {
	return strings.ReplaceAll(s, "'", "'\"'\"'")
}

func waitLsfLog(step *engine.Step) {
	fout := step.Lsf.Shell.Name() + ".out"
	ferr := step.Lsf.Shell.Name() + ".err"
	retry := 0
	for !(fileExist(fout) && fileExist(ferr)) && (step.Lsf.Running || retry < 5) {
		time.Sleep(1000 * time.Millisecond)
		if !step.Lsf.Running {
			retry++
		}
	}
	if !fileExist(fout) {
		return
	}
	retry = 0
	for lsfLogEmpty(fout) && retry < 20 {
		time.Sleep(500 * time.Millisecond)
		retry++
	}
}

func lsfLogEmpty(fout string) bool {
	rd, err := os.Open(fout)
	if err != nil {
		return true
	}
	lc, err2 := lineCounter(rd)
	if err2 != nil {
		return true
	}
	if lc < 10 {
		return true
	}
	return false
}

func lineCounter(r io.Reader) (int, error) {
	buf := make([]byte, 32*1024)
	count := 0
	lineSep := []byte{'\n'}

	for {
		c, err := r.Read(buf)
		count += bytes.Count(buf[:c], lineSep)

		switch {
		case err == io.EOF:
			return count, nil

		case err != nil:
			return count, err
		}
	}
}

func checkPort(port int) (status bool, err error) {

	// Concatenate a colon and the port
	host := ":" + strconv.Itoa(port)
	// Try to create a server with the port
	server, err := net.Listen("tcp", host)
	// if it fails then the port is likely taken
	if err != nil {
		return false, err
	}
	// close the server
	server.Close()
	// we successfully used and closed the port
	// so it's now available to be used again
	return true, nil
}

func checkNetCat(port int) bool {
	var work bool
	var retry int
	var err error
	var out []byte
	retry = 0
	work = false
	for retry < 50 {
		check := exec.Command(
			"sh", "-c",
			fmt.Sprintf("sh -c 'lsof -i :%d | grep -c nc'", port))
		if out, err = check.CombinedOutput(); err != nil {
			retry++
			time.Sleep(100 * time.Millisecond)
			continue
		}
		if strings.Trim(string(out), "\n") != "1" {
			retry++
			time.Sleep(100 * time.Millisecond)
			continue
		}
		work = true
		break
	}
	return work
}

func startNetCat(step *engine.Step) (int, error) {
	var port int
	var err error
	retry := 0
	for retry < 50 && step.Lsf.Running {
		if port, err = freeport.GetFreePort(); err != nil {
			return 0, err
		}
		step.Lsf.NetCat = exec.Command("nc", "-d", "-l", fmt.Sprintf("%d", port))
		step.Lsf.NetCat.Stdout = step.Lsf.NetPipe
		if err = step.Lsf.NetCat.Start(); err != nil {
			return 0, err
		}
		if !checkNetCat(port) {
			step.Lsf.NetCat.Process.Kill()
			step.Lsf.NetCat.Wait()
			retry++
			time.Sleep(100 * time.Millisecond)
			log.Println("Retry new port")
			continue
		}
		return port, nil
	}
	return 0, &errorString{s: "Cannot open netcat"}
}

func timeOutWait(cmd *exec.Cmd) error {
	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
	}()
	select {
	case <-time.After(5 * time.Second):
		if err := cmd.Process.Kill(); err != nil {
			log.Print("failed to kill process: ", err)
		}
		log.Println("process killed as timeout reached")
	case err := <-done:
		if err != nil {
			log.Printf("process finished with error = %v", err)
			return err
		}
		log.Print("process finished successfully")
	}
	return nil
}
