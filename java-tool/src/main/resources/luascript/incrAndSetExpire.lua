local key =  tostring(KEYS[1])
local delta = tonumber(ARGV[1])
local expireTime = tonumber(ARGV[2])
local num = redis.call('incrby',key,delta)
redis.call('pexpire',key,expireTime)
return num


