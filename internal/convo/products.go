package convo

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"bot-jual/internal/atl"
)

var amountRegex = regexp.MustCompile(`\d+(?:[.,]?\d+)?`)

func filterByQuery(items []atl.PriceListItem, query string) []atl.PriceListItem {
	if query == "" {
		return items
	}
	query = strings.ToLower(query)
	var res []atl.PriceListItem
	for _, item := range items {
		score := matchScore(item, query)
		if score > 0 {
			res = append(res, item)
		}
	}
	sort.Slice(res, func(i, j int) bool {
		return res[i].Price < res[j].Price
	})
	if len(res) > 5 {
		return res[:5]
	}
	return res
}

func filterByBudget(items []atl.PriceListItem, budget int64) []atl.PriceListItem {
	var res []atl.PriceListItem
	for _, item := range items {
		if item.Price <= float64(budget) && strings.EqualFold(item.Status, "available") {
			res = append(res, item)
		}
	}
	sort.Slice(res, func(i, j int) bool {
		return res[i].Price < res[j].Price
	})
	if len(res) > 5 {
		return res[:5]
	}
	return res
}

func formatPriceList(items []atl.PriceListItem) string {
	if len(items) == 0 {
		return "Belum ada produk yang cocok."
	}
	lines := make([]string, len(items))
	for i, item := range items {
		lines[i] = fmt.Sprintf("%s (%s) - Rp%.0f [%s]", item.Name, item.Code, item.Price, strings.ToUpper(item.Status))
	}
	return "Ini beberapa opsi:\n- " + strings.Join(lines, "\n- ")
}

func matchScore(item atl.PriceListItem, query string) int {
	name := strings.ToLower(item.Name)
	code := strings.ToLower(item.Code)
	category := strings.ToLower(item.Category)
	provider := strings.ToLower(item.Provider)

	score := 0
	if strings.Contains(name, query) {
		score += 4
	}
	if strings.Contains(code, query) {
		score += 5
	}
	if strings.Contains(category, query) {
		score += 2
	}
	if strings.Contains(provider, query) {
		score += 1
	}
	return score
}

func parseAmount(text string) (int64, error) {
	if text == "" {
		return 0, fmt.Errorf("empty amount")
	}
	text = strings.ToLower(strings.TrimSpace(text))
	matches := amountRegex.FindAllString(text, -1)
	if len(matches) == 0 {
		return 0, fmt.Errorf("no numeric value")
	}
	value := matches[0]
	value = strings.ReplaceAll(value, ".", "")
	value = strings.ReplaceAll(value, ",", "")

	num, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, err
	}
	if strings.Contains(text, "k") && num < 1000 {
		num *= 1000
	}
	if strings.Contains(text, "m") && num < 1000000 {
		num *= 1000000
	}
	return num, nil
}
