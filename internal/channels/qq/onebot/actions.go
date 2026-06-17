package onebot

// Action strings for the OneBot 11 API surface we use. Kept as exported
// constants so the QQ channel never hard-codes action names inline.
const (
	ActionSendPrivateMsg     = "send_private_msg"
	ActionSendGroupMsg       = "send_group_msg"
	ActionDeleteMsg          = "delete_msg"
	ActionGetLoginInfo       = "get_login_info"
	ActionGetGroupMemberList = "get_group_member_list"
	ActionSetMsgEmojiLike    = "set_msg_emoji_like" // reserved for reaction (phase 2)
)

// SendPrivateParams is the params object for send_private_msg.
type SendPrivateParams struct {
	UserID  int64       `json:"user_id"`
	Message []MessageSegment `json:"message"`
}

// SendGroupParams is the params object for send_group_msg.
type SendGroupParams struct {
	GroupID int64       `json:"group_id"`
	Message []MessageSegment `json:"message"`
}

// DeleteMsgParams is the params object for delete_msg.
type DeleteMsgParams struct {
	MessageID int64 `json:"message_id"`
}

// GroupMemberListParams is the params object for get_group_member_list.
type GroupMemberListParams struct {
	GroupID int64 `json:"group_id"`
}
