#Performs find and replace in multiple files at a time

vim <partial filename*>

#in vim
:set aw #auto-write
:argdo %s/oldstring/newstring/g
