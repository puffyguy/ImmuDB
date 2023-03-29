package immudb

import (
	"context"
	"fmt"

	"github.com/codenotary/immudb/pkg/api/schema"
	immuclient "github.com/codenotary/immudb/pkg/client"
)

var (
	Options = immuclient.DefaultOptions().WithAddress("localhost").WithPort(3322)
	Client  = immuclient.NewClient().WithOptions(Options)
	Ctx     = context.Background()
)

func EstablishConnection() (string, error) {
	err := Client.OpenSession(Ctx, []byte("immudb"), []byte("Immudb@12"), "defaultdb")
	if err != nil {
		fmt.Println("Error while opening connection to DB:", err)
		return "", err
	}
	defer Client.CloseSession(Ctx)
	return "Connection Success..", nil
}

func GetActiveDB() (string, error) {
	databaseSettings, err := Client.GetDatabaseSettingsV2(Ctx)
	if err != nil {
		fmt.Println("Error while getting active database:", err)
		return "", err
	}
	return databaseSettings.Database, nil
}

func ListAllDB() ([]string, error) {
	databaseList, err := Client.DatabaseListV2(Ctx)
	if err != nil {
		fmt.Println("Error while getting list of database:", err)
		return []string{}, err
	}
	dbs := []string{}
	for _, v := range databaseList.Databases {
		dbs = append(dbs, v.Name)
	}
	return dbs, nil
}

func UseDB(dbName string) (string, error) {
	_, err := Client.UseDatabase(Ctx, &schema.Database{
		DatabaseName: dbName,
	})
	if err != nil {
		fmt.Println("Error while changing DB:", err)
		return "", err
	}
	currentDB, err := GetActiveDB()
	if err != nil {
		fmt.Println("Error while getting active database:", err)
		return "", err
	}
	return "Now using" + currentDB, nil
}
