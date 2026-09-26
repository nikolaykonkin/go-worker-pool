package main

import (
	"fmt"
	"hash/fnv"
	"sort"
	"sync"
	"time"
)

// Job — задание для воркера: URL, который нужно "обработать"
type Job struct {
	ID  int
	URL string
}

// Result — результат обработки задания
type Result struct {
	Job      Job
	Status   string
	Duration time.Duration
	WorkerID int
}

const workerCount = 5

// fetchURL имитирует отправку HTTP-запроса со случайной задержкой
// Длительность зависит от URL: хеш от строки добавляет вариацию,
// имитируя разное время ответа у разных эндпоинтов
func fetchURL(url string) (status string, duration time.Duration) {
	start := time.Now()

	h := fnv.New32a()
	h.Write([]byte(url))
	base := int(h.Sum32() % 400) // 0–399

	// Задержка 100–499 мс, детерминированная для конкретного URL
	delay := time.Duration(100+base) * time.Millisecond
	time.Sleep(delay)

	return "обработан", time.Since(start)
}

// worker читает задания из канала jobs, обрабатывает их и отправляет результат в канал results
// По завершении цикла (канал jobs закрыт) сигнализирует о завершении через wg.Done()
func worker(id int, jobs <-chan Job, results chan<- Result, wg *sync.WaitGroup) {
	defer wg.Done()

	for job := range jobs {
		status, duration := fetchURL(job.URL)
		results <- Result{
			Job:      job,
			Status:   status,
			Duration: duration,
			WorkerID: id,
		}
	}
}

func main() {
	start := time.Now()

	urls := []string{
		"https://example.com",
		"https://golang.org",
		"https://github.com",
		"https://stackoverflow.com",
		"https://google.com",
		"https://wikipedia.org",
		"https://reddit.com",
		"https://ycombinator.com",
		"https://docs.go.dev",
		"https://netology.ru",
		"https://habr.com",
		"https://apple.com",
	}

	jobs := make(chan Job, len(urls))
	results := make(chan Result, len(urls))

	var wg sync.WaitGroup

	// Fan-out: запускаем фиксированное количество воркеров (Worker Pool),
	// каждый читает задания из общего канала jobs
	for w := 1; w <= workerCount; w++ {
		wg.Add(1)
		go worker(w, jobs, results, &wg)
	}

	// Наполняем канал заданиями на основе списка URL и закрываем его,
	// чтобы воркеры знали, что новых заданий больше не будет
	for i, url := range urls {
		jobs <- Job{ID: i + 1, URL: url}
	}
	close(jobs)

	// Fan-in: отдельная горутина дожидается завершения всех воркеров и закрывает канал results,
	// чтобы главная горутина могла корректно завершить чтение через for range
	go func() {
		wg.Wait()
		close(results)
	}()

	// Главная горутина собирает все результаты из канала results
	collected := make([]Result, 0, len(urls))
	for res := range results {
		collected = append(collected, res)
	}

	elapsed := time.Since(start)
	printReport(collected, elapsed)
}

// printReport выводит агрегированный отчет по всем обработанным URL:
// список результатов (отсортированный по ID задания для читаемости),
// итоговую статистику и фактическое время выполнения программы
func printReport(results []Result, elapsed time.Duration) {
	sort.Slice(results, func(i, j int) bool {
		return results[i].Job.ID < results[j].Job.ID
	})

	fmt.Println("=== Отчёт по обработке URL ===")
	fmt.Println()

	var total time.Duration
	successCount := 0

	for _, r := range results {
		fmt.Printf("[%2d] %-30s статус: %-10s время: %-8v воркер: %d\n",
			r.Job.ID, r.Job.URL, r.Status, r.Duration.Round(time.Millisecond), r.WorkerID)

		total += r.Duration
		if r.Status == "обработан" {
			successCount++
		}
	}

	fmt.Println()
	fmt.Println("=== Итоговая статистика ===")
	fmt.Printf("Всего обработано: %d\n", len(results))
	fmt.Printf("Успешно: %d\n", successCount)
	if len(results) > 0 {
		avg := total / time.Duration(len(results))
		fmt.Printf("Среднее время обработки: %v\n", avg.Round(time.Millisecond))
	}
	fmt.Printf("Суммарное время (последовательно): %v\n", total.Round(time.Millisecond))
	fmt.Printf("Фактическое время выполнения: %v\n", elapsed.Round(time.Millisecond))
}
