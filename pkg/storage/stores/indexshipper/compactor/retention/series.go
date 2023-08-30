package retention

import (
	"github.com/prometheus/prometheus/model/labels"
)

type UserSeries struct {
	key         []byte
	seriesIDLen int
}

func newUserSeries(seriesID []byte, userID []byte) UserSeries {
	key := make([]byte, 0, len(seriesID)+len(userID))
	key = append(key, seriesID...)
	key = append(key, userID...)
	return UserSeries{
		key:         key,
		seriesIDLen: len(seriesID),
	}
}

func (us UserSeries) Key() string {
	return unsafeGetString(us.key)
}

func (us UserSeries) SeriesID() []byte {
	return us.key[:us.seriesIDLen]
}

func (us UserSeries) UserID() []byte {
	return us.key[us.seriesIDLen:]
}

func (us *UserSeries) Reset(seriesID []byte, userID []byte) {
	if us.key == nil {
		us.key = make([]byte, 0, len(seriesID)+len(userID))
	}
	us.key = us.key[:0]
	us.key = append(us.key, seriesID...)
	us.key = append(us.key, userID...)
	us.seriesIDLen = len(seriesID)
}

type UserSeriesInfo struct {
	UserSeries
	IsDeleted bool
	Labels    labels.Labels
}

type UserSeriesMap map[string]UserSeriesInfo

func NewUserSeriesMap() UserSeriesMap {
	return make(UserSeriesMap)
}

func (u UserSeriesMap) Add(seriesID []byte, userID []byte, lbls labels.Labels) {
	us := newUserSeries(seriesID, userID)
	if _, ok := u[us.Key()]; ok {
		return
	}

	u[us.Key()] = UserSeriesInfo{
		UserSeries: us,
		IsDeleted:  true,
		Labels:     lbls,
	}
}

// MarkSeriesNotDeleted is used to mark series not deleted when it still has some chunks left in the store
func (u UserSeriesMap) MarkSeriesNotDeleted(seriesID []byte, userID []byte) {
	us := newUserSeries(seriesID, userID)
	usi := u[us.Key()]
	usi.IsDeleted = false
	u[us.Key()] = usi
}

func (u UserSeriesMap) ForEach(callback func(info UserSeriesInfo) error) error {
	for _, v := range u {
		if err := callback(v); err != nil {
			return err
		}
	}
	return nil
}
