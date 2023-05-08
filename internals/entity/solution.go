package entity

type Solution struct {
	TaskID int    `json:"task_id"`
	UserID int    `json:"user_id"`
	Code   string `json:"solution"`
}
