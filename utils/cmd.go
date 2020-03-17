package utils

import (
	"bytes"
	"fmt"
	"os/exec"
)

// RedisCliCmd method to run redis-cli commands.
func RedisCliCmd(commandParams []string) error {

	cmd := exec.Command("redis-cli", commandParams...)

	fmt.Printf("Command to run %+v\n", cmd)
	var out bytes.Buffer

	cmd.Stdout = &out
	cmd.Stderr = &out

	err := cmd.Run()
	if err != nil {
		fmt.Printf("Failed to execute command.\n%v\n", out.String())
		return err
	}
	fmt.Printf("Successfully executed command.\n%v\n", out.String())
	return nil
}
