package handlers

import (
	"io"
	"net/http"
)

// IsAlive godoc
// @Summary Check if alive
// @Description allows to check if the assistant is alive
// @Tags System
// @Success 200  "Status OK"
// @Router /isalive [get]
func IsAlive(w http.ResponseWriter, r *http.Request) {
	io.WriteString(w, `{"alive": true}`)
}

// NotImplemented returns a basic message "Not Implemented" when called, and should be use a filler for future handler
func NotImplemented(w http.ResponseWriter, r *http.Request) {
	io.WriteString(w, `{"message":"Not Implemented"}`)
}

// FuncLogin godoc (only for swagger doc)
// @Summary Login
// @Description Authenticate using basic auth
// @Description Example :
// @Description <pre>{"login":"myuser","password":"mypassword"}</pre>
// @Tags Security
// @Produce json
// @Param job body interface{} true "Credentials (json)"
// @Success 200 "Status OK"
// @Failure 400 "Status Bad Request"
// @Router /login [post]
func FuncLogin() {}

// FuncGetLogLevel godoc (only for swagger doc)
// @Summary Get Log Level
// @Description Get current logging level
// @Tags Logs
// @Produce json
// @Success 200 "Status OK"
// @Failure 400 "Status Bad Request"
// @Router /log_level [get]
func FuncGetLogLevel() {}

// FuncSetLogLevel godoc (only for swagger doc)
// @Summary Set Log Level
// @Description Set logging level
// @Description Example :
// @Description <pre>{"level":"info"}</pre>
// @Tags Logs
// @Consumme json
// @Produce json
// @Param level body interface{} true "Level (json)"
// @Success 200 "Status OK"
// @Failure 400 "Status Bad Request"
// @Router /log_level [put]
func FuncSetLogLevel() {}
