package main

import (
	"context"
	"flag"
	"fmt"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/sheets/v4"
	"google.golang.org/api/youtube/v3"
	"log"
	"net/http"
	"os"
	"path"
	"strings"
)

type VideoData struct {
	Filename string
	Title string
	Description string
	Category string
	Keywords string
}

const (
	CLIENT_ID string = "xxx"
	CLIENT_SECRET string = "xxx"

	SPREADSHEET_ID string = "xxx"
)

func NewVideoData(row []interface{}, dir string) VideoData {
	// 0                1			2			3				4		5			6										7		8		9
	// Kazetta sorszáma	Dátum		Kategória	Cím/tartalom	Kezdete	Vége		__filename								__ss	__to	__output
	// 40	            1997.06.07	beac		BEAC 97			0:01:09	4:55:08		apu_kazettak_part2_180206/Kicsi_40.mpg	0:01.09	4:55.08	out_2.mpg
	vd := VideoData{
		Filename: path.Join(dir, row[9].(string)),
		Keywords: row[2].(string),
		Category: "22",
		Description: fmt.Sprintf("%s: %s-%s", row[6], row[7], row[8]),
		Title: fmt.Sprintf("%s %s", row[1], row[3]),
	}
	return vd
}

func getClient() *http.Client {
	ctx := context.Background()
	var conf = &oauth2.Config{
		ClientID:     CLIENT_ID,
		ClientSecret: CLIENT_SECRET,
		Endpoint:     google.Endpoint,
		RedirectURL:  "http://localhost:8080",
		Scopes:       []string{youtube.YoutubeUploadScope, sheets.SpreadsheetsScope},
	}

	url := conf.AuthCodeURL("state", oauth2.AccessTypeOffline)
	fmt.Printf("Visit the URL for the auth dialog and then paste the 'code' from the response here: %v\n", url)

	var code string
	if _, err := fmt.Scan(&code); err != nil {
		log.Fatal(err)
	}
	tok, err := conf.Exchange(ctx, code)
	if err != nil {
		log.Fatal(err)
	}

	return conf.Client(ctx, tok)
}

func upload(client *http.Client, vd *VideoData) string {
	service, err := youtube.New(client)
	if err != nil {
		log.Fatalf("Error creating YouTube client: %v", err)
	}

	upload := &youtube.Video{
		Snippet: &youtube.VideoSnippet{
			Title:       vd.Title,
			Description: vd.Description,
			CategoryId:  vd.Category,
		},
		Status: &youtube.VideoStatus{PrivacyStatus: "private"},
	}

	// The API returns a 400 Bad Request response if tags is an empty string.
	if strings.Trim(vd.Keywords, "") != "" {
		upload.Snippet.Tags = strings.Split(vd.Keywords, ",")
	}

	call := service.Videos.Insert("snippet,status", upload)

	file, err := os.Open(vd.Filename)
	defer file.Close()
	if err != nil {
		log.Fatalf("Error opening %v: %v", vd.Filename, err)
	}

	response, err := call.Media(file).Do()
	if err != nil {
		log.Fatalf("Error making YouTube API call: %v", err)
	}
	return response.Id
}

func getRow(client *http.Client, dir string, line int) *VideoData {
	srv, err := sheets.New(client)
	if err != nil {
		log.Fatalf("Unable to retrieve Sheets client: %v", err)
	}

	readRange := fmt.Sprintf("Sheet1!A%d:J%d", line, line)
	resp, err := srv.Spreadsheets.Values.Get(SPREADSHEET_ID, readRange).Do()
	if err != nil {
		log.Fatalf("Unable to retrieve data from sheet: %v", err)
	}


	if len(resp.Values) != 1 {
		log.Fatalf("1 row expected...")
	}
	row := resp.Values[0]
	vd := NewVideoData(row, dir)
	return &vd
}

func setVideoUrl(client *http.Client, line int, url string) {
	srv, err := sheets.New(client)
	if err != nil {
		log.Fatalf("Unable to retrieve Sheets client: %v", err)
	}

	values := [][]interface{}{
		{
			url,
		},
	}

	valueRange := sheets.ValueRange{
		Values: values,

	}

	_, err = srv.Spreadsheets.Values.Update(SPREADSHEET_ID, fmt.Sprintf("Sheet1!L%d:L%d", line, line), &valueRange).ValueInputOption("RAW").Do()
	if err != nil {
		log.Fatalf("Unable to set data in spreadsheet: %v", err)
	}

}

func singleUpload(client *http.Client, dir string, line int) {
	vd := getRow(client, dir, line)

	var videoUrl string
	if ok, err := isFileSizeOk(vd.Filename); !ok {
		if err != nil {
			videoUrl = err.Error()
		} else {
			videoUrl = "file too small, likely ffmpeg error"
		}
	} else {
		videoId := upload(client, vd)
		videoUrl = fmt.Sprintf("https://youtu.be/%s", videoId)
	}

	setVideoUrl(client, line, videoUrl)
	fmt.Printf("done with %d\n", line)
}

func uploadWorker(client *http.Client, dir string, jobs <- chan int, results chan<- int) {
	for j := range jobs {
		singleUpload(client, dir, j)
		results <- j
	}
}

func isFileSizeOk(path string) (bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return false, err
	}

	fileInfo, err := f.Stat()
	if err != nil {
		return false, err
	}

	if fileInfo.Size() > 1024*1024 {
		return true, nil
	}
	return false, nil
}

func main() {
	dir := flag.String("dir", "", "Directory containing videos.")
	from := flag.Int("from", 0, "First line to process.")
	to := flag.Int("to", 0, "Last line to process.")

	flag.Parse()

	if *dir == "" || *from == 0 || *to == 0 {
		flag.Usage()
		log.Fatal("Please provide all arguments!")
	}

	client := getClient()

	jobs := make(chan int, 1000)
	results := make(chan int, 1000)

	for i := 0; i < 3; i ++ {
		go uploadWorker(client, *dir, jobs, results)
	}

	for i := *from; i <= *to; i ++ {
		jobs <- i
	}
	close(jobs)

	for i := *from; i <= *to; i ++ {
		<- results
	}

	// singleUpload(129)
}