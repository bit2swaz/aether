package main
import (
	"fmt"
	"os"
	"github.com/spf13/cobra"
)
var rootCmd = &cobra.Command{
	Use:   "aether",
	Short: "Aether - Distributed PostgreSQL-compatible database",
	Long: `Aether is a distributed database system that implements the PostgreSQL wire protocol
and uses Raft consensus for replication and high availability.`,
	SilenceUsage: true,
}
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func main() {
	Execute()
}
func init() {
	rootCmd.AddCommand(startCmd)
}
func printBanner() {
	banner := `
    ___    ______ _____ __  __ _____ ___
   /   |  / ____/_  __ 
  / /| | / __/   / /  / /_/ / __/ / /_/ /
 / ___ |/ /___  / /  / __  / /___/ _, _/
/_/  |_/_____/ /_/  /_/ /_/_____/_/ |_|
Distributed PostgreSQL-compatible Database
`
	fmt.Println(banner)
}
