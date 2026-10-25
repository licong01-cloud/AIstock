package main

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
)

var buildRevision = "unknown"

func explicitDatabaseDSN() (string, error) {
	if dsn := strings.TrimSpace(os.Getenv("TDX_DB_DSN")); dsn != "" {
		return dsn, nil
	}
	for _, key := range []string{"HOST", "PORT", "NAME", "USER", "PASSWORD"} {
		if os.Getenv("TDX_DB_"+key) == "" {
			return "", fmt.Errorf("missing explicit TDX_DB_%s", key)
		}
	}
	port, err := strconv.Atoi(os.Getenv("TDX_DB_PORT"))
	if err != nil || port < 1 || port > 65535 {
		return "", errors.New("invalid TDX_DB_PORT")
	}
	u := url.URL{Scheme: "postgres", Host: os.Getenv("TDX_DB_HOST") + ":" + strconv.Itoa(port), Path: "/" + os.Getenv("TDX_DB_NAME"), User: url.UserPassword(os.Getenv("TDX_DB_USER"), os.Getenv("TDX_DB_PASSWORD"))}
	return u.String(), nil
}

func listenAddress() (string, error) {
	p := strings.TrimSpace(os.Getenv("TDX_HTTP_PORT"))
	if p == "" {
		p = "8080"
	}
	p = strings.TrimPrefix(strings.TrimPrefix(p, "tcp/"), ":")
	if strings.HasPrefix(p, "http://") || strings.HasPrefix(p, "https://") {
		u, err := url.Parse(p)
		if err != nil {
			return "", errors.New("invalid TDX_HTTP_PORT")
		}
		p = u.Port()
	}
	n, err := strconv.Atoi(p)
	if err != nil || n < 1 || n > 65535 {
		return "", errors.New("invalid TDX_HTTP_PORT")
	}
	host := os.Getenv("TDX_HTTP_HOST")
	if os.Getenv("TDX_RUNTIME_ENV") == "dev" {
		if (n != 19081 && n != 19082) || host != "127.0.0.1" || os.Getenv("TDX_DB_NAME") != "aistock_dev" || os.Getenv("TDX_DB_DSN") != "" {
			return "", errors.New("isolated DEV requires loopback validation port and explicit existing DEV database")
		}
	}
	return host + ":" + strconv.Itoa(n), nil
}
