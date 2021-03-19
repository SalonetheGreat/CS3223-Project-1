file = open("Schedule.txt", "r")
lines = file.readlines()

s = set(lines)

print(len(s) == len(lines))
