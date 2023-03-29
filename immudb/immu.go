package immudb

import (
	"context"
	"fmt"

	immuclient "github.com/codenotary/immudb/pkg/client"
)

var (
	Options = immuclient.DefaultOptions().WithAddress("localhost").WithPort(3322)
	Client  = immuclient.NewClient().WithOptions(Options)
	Ctx     = context.Background()
)

func EstablishConnection() {
	err := Client.OpenSession(Ctx, []byte("immudb"), []byte("Immudb@12"), "defaultdb")
	if err != nil {
		fmt.Println("Error while opening connection to DB:", err)
		return
	}
	fmt.Println("Connection Success..")
	defer Client.CloseSession(Ctx)
}
