module github.com/davidhrinaldo/billet/example/chirpstack

go 1.26.1

require (
	github.com/chirpstack/chirpstack/api/go/v4 v4.10.2
	github.com/davidhrinaldo/billet v0.0.0
	google.golang.org/grpc v1.72.1
)

require (
	golang.org/x/net v0.35.0 // indirect
	golang.org/x/sys v0.30.0 // indirect
	golang.org/x/text v0.22.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20250218202821-56aae31c358a // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250218202821-56aae31c358a // indirect
	google.golang.org/protobuf v1.36.5 // indirect
)

replace github.com/davidhrinaldo/billet => ../..
