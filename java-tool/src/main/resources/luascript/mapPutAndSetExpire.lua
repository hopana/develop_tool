local keyStr = KEYS[1]
 --按照“,”分割字符串
local t={}
for w in string.gmatch(keyStr,"([^',']+)") do
    table.insert(t,w)
end
local key =  t[1];
local expireTime = tonumber(t[2])
local fieldKey = t[3]
redis.call('hmset',key,fieldKey,ARGV[1])
redis.call('pexpire',key,expireTime)
return 0