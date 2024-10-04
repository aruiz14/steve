package accesscontrol

import (
	"context"
	"slices"
	"sort"
	"time"

	v1 "github.com/rancher/wrangler/v3/pkg/generated/controllers/rbac/v1"
	"golang.org/x/sync/singleflight"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/util/cache"
	"k8s.io/apiserver/pkg/authentication/user"
)

//go:generate mockgen --build_flags=--mod=mod -package fake -destination fake/AccessSetLookup.go "github.com/rancher/steve/pkg/accesscontrol" AccessSetLookup

type AccessSetLookup interface {
	AccessFor(user user.Info) *AccessSet
	PurgeUserData(id string)
}

type policyRules interface {
	get(string) *AccessSet
	getRoleBindings(string) []*rbacv1.RoleBinding
	getClusterRoleBindings(string) []*rbacv1.ClusterRoleBinding
	getRoleRefs(subjectName string) subjectGrants
}

type roleRevisions interface {
	roleRevision(string, string) string
}

// accessStoreCache is a subset of the methods implemented by LRUExpireCache
type accessStoreCache interface {
	Add(key interface{}, value interface{}, ttl time.Duration)
	Get(key interface{}) (interface{}, bool)
	Remove(key interface{})
}

type AccessStore struct {
	usersPolicyRules    policyRules
	groupsPolicyRules   policyRules
	roles               roleRevisions
	cache               accessStoreCache
	concurrentAccessFor *singleflight.Group
}

type roleKey struct {
	namespace string
	name      string
}

func NewAccessStore(ctx context.Context, cacheResults bool, rbac v1.Interface) *AccessStore {
	as := &AccessStore{
		usersPolicyRules:    newPolicyRuleIndex(true, rbac),
		groupsPolicyRules:   newPolicyRuleIndex(false, rbac),
		roles:               newRoleRevision(ctx, rbac),
		concurrentAccessFor: new(singleflight.Group),
	}
	if cacheResults {
		as.cache = cache.NewLRUExpireCache(50)
	}
	return as
}

func (l *AccessStore) AccessFor(user user.Info) *AccessSet {
	info := l.toUserInfo(user)
	if l.cache == nil {
		return l.newAccessSet(info)
	}

	cacheKey := info.hash()

	res, _, _ := l.concurrentAccessFor.Do(cacheKey, func() (interface{}, error) {
		if val, ok := l.cache.Get(cacheKey); ok {
			as, _ := val.(*AccessSet)
			return as, nil
		}

		result := l.newAccessSet(info)
		result.ID = cacheKey
		l.cache.Add(cacheKey, result, 24*time.Hour)

		return result, nil
	})
	return res.(*AccessSet)
}

func (l *AccessStore) newAccessSet(info userGrants) *AccessSet {
	result := info.user.toAccessSet()
	for _, group := range info.groups {
		result.Merge(group.toAccessSet())
	}
	return result
}

func (l *AccessStore) PurgeUserData(id string) {
	l.cache.Remove(id)
}

// toUserInfo retrieves all the access information for a user
func (l *AccessStore) toUserInfo(user user.Info) userGrants {
	var res userGrants

	groups := slices.Clone(user.GetGroups())
	sort.Strings(groups)

	res.user = l.usersPolicyRules.getRoleRefs(user.GetName())
	for _, group := range groups {
		res.groups = append(res.groups, l.groupsPolicyRules.getRoleRefs(group))
	}

	return res
}
