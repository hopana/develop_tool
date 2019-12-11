local key =  tostring(KEYS[1])
local delta = tonumber(ARGV[1])
local expireTime = tonumber(ARGV[2])
local num = redis.call('incrby',key,delta)
if(redis.call('pttl',key) == -1) then
    redis.call('pexpire',key,expireTime)
end
return num


