// Command gohelper drives the Go eddy library from the Python tests: it
// creates River's tables, and acts as a Go API or a Go controller on the
// same database so the two libraries can be checked against each other.
//
//	gohelper <database-url> <schema> migrate
//	gohelper <database-url> <schema> check
//	gohelper <database-url> <schema> insert <name> <image>
//	gohelper <database-url> <schema> delete <id>
//	gohelper <database-url> <schema> run
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Anhui-tqhuang/eddy"
)

// App mirrors the model in the Python tests.
type App struct {
	ID             uuid.UUID `db:",default"`
	Name           string
	Image          string
	CreatedAt      time.Time  `db:",default"`
	Phase          string     `db:",status"`
	ReconcileError *string    `db:",error"`
	ReconciledAt   *time.Time `db:",reconciled"`
}

var apps = eddy.NewTable[App]("apps")

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "gohelper:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) < 3 {
		return fmt.Errorf("usage: gohelper <database-url> <schema> <command> [args]")
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg, err := pgxpool.ParseConfig(args[0])
	if err != nil {
		return err
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = args[1]
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return err
	}
	defer pool.Close()

	opts := []eddy.Option{eddy.WithLogger(slog.New(slog.DiscardHandler))}
	if args[2] == "check" {
		// Fail, rather than migrate, unless River's tables are already current.
		opts = append(opts, eddy.WithoutMigrate())
	}
	db, err := eddy.Open(ctx, pool, opts...) // creates River's tables
	if err != nil {
		return err
	}

	switch cmd, rest := args[2], args[3:]; cmd {
	case "migrate", "check":
		return nil
	case "insert":
		if len(rest) != 2 {
			return fmt.Errorf("insert needs <name> <image>")
		}
		app := App{Name: rest[0], Image: rest[1]}
		if err := apps.Insert(ctx, db, &app); err != nil {
			return err
		}
		fmt.Println(app.ID)
		return nil
	case "delete":
		if len(rest) != 1 {
			return fmt.Errorf("delete needs <id>")
		}
		id, err := uuid.Parse(rest[0])
		if err != nil {
			return err
		}
		return apps.Delete(ctx, db, id)
	case "run":
		return db.Run(ctx,
			eddy.Reconcile(apps, func(_ context.Context, app *App) error {
				app.Phase = "go:" + app.Image
				return nil
			}),
			eddy.Teardown(apps, func(ctx context.Context, app *App) error {
				_, err := pool.Exec(ctx,
					"INSERT INTO torn (by, name, image, phase, created_at) VALUES ('go', $1, $2, $3, $4)",
					app.Name, app.Image, app.Phase, app.CreatedAt)
				return err
			}),
			eddy.OnLeadershipChange(func(leading bool) {
				if leading {
					fmt.Println("leading")
				}
			}),
			eddy.WithResync(0),
		)
	default:
		return fmt.Errorf("unknown command %q", cmd)
	}
}
