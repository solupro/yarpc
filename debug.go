package yarpc

import (
	"fmt"
	"html/template"
	"net/http"
)

const DEBUG_TEXT = `<html>
	<body>
	<title>YARPC Services</title>
	{{range .}}
	<hr>
	Service {{.Name}}
	<hr>
		<table>
		<th align=center>Method</th><th align=center>Calls</th>
		{{range $name, $mtype := .Method}}
			<tr>
			<td align=left font=fixed>{{$name}}({{$mtype.ArgType}}, {{$mtype.ReplyType}}) error</td>
			<td align=center>{{$mtype.NumCalls}}</td>
			</tr>
		{{end}}
		</table>
	{{end}}
	</body>
	</html>`

var debug = template.Must(template.New("RPC debug").Parse(DEBUG_TEXT))

type debugHTTP struct {
	*Server
}

type debugService struct {
	Name   string
	Method map[string]*methodType
}

func (d *debugHTTP) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var services []debugService
	d.serviceMap.Range(func(name, svci interface{}) bool {
		svc := svci.(*service)
		services = append(services, debugService{
			Name:   name.(string),
			Method: svc.method,
		})
		return true
	})

	err := debug.Execute(w, services)
	if nil != err {
		_, _ = fmt.Fprintln(w, "rpc: error executing template:", err.Error())
	}
}
