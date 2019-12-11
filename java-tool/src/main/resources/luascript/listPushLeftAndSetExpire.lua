local keyStr = KEYS[1]
 --按照“,”分割字符串
local t={}
for w in string.gmatch(keyStr,"([^',']+)") do
    table.insert(t,w)
end
local key =  t[1];
local expireTime = tonumber(t[2])
local num = 0
for i,value in ipairs(ARGV) do
    num = num +  redis.call('lpush',key,value)
end

redis.call('pexpire',key,expireTime)
return num