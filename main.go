package main

import (
	"bytes"
	"fmt"
	"github.com/docopt/docopt-go"
	azure "launchpad.net/gwacl"
	"log"
	"os/exec"
	"time"
)

var args map[string]interface{}

func main() {
	dependencies := []string{"rtmpdump", "ffmpeg"}
	for _, e := range dependencies {
		_, err := exec.LookPath(e)
		if err != nil {
			log.Fatal(err)
		}
	}

	usage := `RTMP dump & encode & uploader

Listens to a RTMP stream for specified duration, encodes to specified audio format and uploads to Azure
Blob Storage.

Usage:
  rtmpsave [options]

Options:
  --azureAccount=<account>         Azure Blob Storage Account Name
  --azureKey=<key>                 Azure Blob Storage Account Key
  --azureContainer=<container>     Azure Blob Storage Container name to upload file
  --rtmpUrl=<url>                  RTMP stream URL
  --rtmpDuration=<seconds>         Duration to listen for RTMP stream
  --audioSampleRate=48000=<rate>   Sample rate for output audio (e.g. 48000)
  --audioDataRate=96k=<rate>       Data rate for output audio (e.g. 96k)
  --audioChannels=2=<channels>     Channel count for output audio (e.g. 2)
  --audioOutputFormat=<format>     Format for output audio, as well as the blob name extension [default: mp3]
`
	args, err := docopt.Parse(usage, nil, true, "", true)
	if err != nil {
		log.Fatal(err)
	}

	rtmpdump := exec.Command("rtmpdump", "-v", "-r", argument(args, "--rtmpUrl"), "--stop", argument(args, "--rtmpDuration"))
	ffmpeg := exec.Command("ffmpeg", "-y", "-i", "pipe:0", "-ar", argument(args, "--audioSampleRate"), "-ab",
		argument(args, "--audioDataRate"), "-ac", argument(args, "--audioChannels"), "-f", argument(args, "--audioOutputFormat"), "-")
	var rtmpdumpErr bytes.Buffer
	rtmpdump.Stderr = &rtmpdumpErr
	ffmpeg.Stdin, err = rtmpdump.StdoutPipe()
	if err != nil {
		log.Fatal(err)
	}
	var ffmpegOut, ffmpegErr bytes.Buffer
	ffmpeg.Stdout = &ffmpegOut
	ffmpeg.Stderr = &ffmpegErr

	err = ffmpeg.Start()
	if err != nil {
		log.Printf("%s\n", ffmpegErr.String())
		log.Println("ffmpeg.Start() failed.")
		log.Fatal(err)
	}
	err = rtmpdump.Run()
	if err != nil {
		log.Printf("%s\n", rtmpdumpErr.String())
		log.Println("rtmpdump.Run() failed")
		log.Fatal(err)
	}
	err = ffmpeg.Wait()
	if err != nil {
		log.Printf("%s\n", ffmpegErr.String())
		log.Println("ffmpeg.Wait() failed")
		log.Fatal(err)
	}

	storage := newStorageClient(argument(args, "--azureAccount"), argument(args, "--azureKey"))
	blobName := blobName(nowDateUTC(), argument(args, "--audioOutputFormat"))
	err = storage.UploadBlockBlob(argument(args, "--azureContainer"), blobName, &ffmpegOut)
	if err != nil {
		log.Printf("Error while uploading blob.")
		log.Fatal(err)
	}
	log.Printf("Blob successfully uploaded as %s\n", blobName)
}

func argument(m map[string]interface{}, name string) string {
	v, ok := m[name]
	if !ok {
		log.Fatalf("Argument missing: %s", name)
	}
	if v == nil {
		log.Fatalf("Argument not specified: %s", name)
	}
	return fmt.Sprintf("%v", v)
}

func nowDateUTC() string {
	return time.Now().UTC().Truncate(time.Hour).Format(time.RFC3339)
}

func blobName(date, extension string) string {
	return fmt.Sprintf("%s.%s", date, extension)
}

func newStorageClient(name, key string) *azure.StorageContext {
	return &azure.StorageContext{Account: name,
		Key:           key,
		AzureEndpoint: azure.GetEndpoint(""),
		RetryPolicy:   azure.RetryPolicy{NbRetries: 3, Delay: 10, HttpStatusCodes: []int{409, 500, 501, 502, 503}}}
}
