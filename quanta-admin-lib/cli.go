package admin

var Cli struct {
	ConsulAddr  string         `default:"127.0.0.1:8500"`
	Port        int            `default:"4000"`
	Debug       bool           `default:"false"`
	Create      CreateCmd      `cmd:"" help:"Create table."`
	Drop        DropCmd        `cmd:"" help:"Drop table."`
	Truncate    TruncateCmd    `cmd:"" help:"Truncate table."`
	Status      StatusCmd      `cmd:"" help:"Show status."`
	Version     VersionCmd     `cmd:"" help:"Show version."`
	Tables      TablesCmd      `cmd:"" help:"Show tables."`
	Shutdown    ShutdownCmd    `cmd:"" help:"Shutdown cluster or one node."`
	FindKey     FindKeyCmd     `cmd:"" help:"Find nodes for key debug tool."`
	Config      ConfigCmd      `cmd:"" help:"Configuration key/value pair."`
	Verify      VerifyCmd      `cmd:"" help:"Verify data for key debug tool."`
	VerifyEnum  VerifyEnumCmd  `cmd:"" help:"Verify a string enum for key debug tool."`
	VerifyIndex VerifyIndexCmd `cmd:"" help:"Verify indices debug tool."`
}
