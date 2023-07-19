from crawler_new import *
import os

#setting
# start_index = 198
# end_index = 4049

# board = 'HatePolitics'
board = 'Gossiping'
# board = 'ID_Multi'
#end of setting

# id
# start_index = 330 #04/01
# end_index = 333 #05/31

# hate
# start_index = 2906 #06/01
# end_index = 4009 #07/31

# start_index = 4009 #04/01
# end_index = 4179 #05/31

# gossip
start_index = 9480 #06/01 33258
end_index = 37757 #07/31

# start_index = 38925 #04/01
# end_index = 39707 #05/31
if(not os.path.isdir('./data/')):
    os.mkdir('./data/')

    
c = PttWebCrawler(as_lib=True)
for i in range (start_index, end_index+1):
    status = None
    while status is None:
        try:
            c.parse_articles(i, i, board)
            status = True
        except:
            pass
