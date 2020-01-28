import logging
from datamodel.search.Sanghuk2Newmanc1Carletoz_datamodel import Sanghuk2Newmanc1CarletozLink, OneSanghuk2Newmanc1CarletozUnProcessedLink
from spacetime.client.IApplication import IApplication
from spacetime.client.declarations import Producer, GetterSetter, Getter
from lxml import html,etree
import re, os
from time import time
from uuid import uuid4

from urlparse import urlparse, parse_qs
from uuid import uuid4
from collections import defaultdict
subdomain_dict = defaultdict(set)
outlinks_dict = defaultdict(set)

logger = logging.getLogger(__name__)
LOG_HEADER = "[CRAWLER]"

@Producer(Sanghuk2Newmanc1CarletozLink)
@GetterSetter(OneSanghuk2Newmanc1CarletozUnProcessedLink)
class CrawlerFrame(IApplication):
    app_id = "Sanghuk2Newmanc1Carletoz"

    def __init__(self, frame):
        self.app_id = "Sanghuk2Newmanc1Carletoz"
        self.frame = frame
        
    def initialize(self):
        self.count = 0
        links = self.frame.get_new(OneSanghuk2Newmanc1CarletozUnProcessedLink)
        if len(links) > 0:
            print "Resuming from the previous state."
            self.download_links(links)
        else:
            l = Sanghuk2Newmanc1CarletozLink("http://www.ics.uci.edu/")
            print l.full_url
            self.frame.add(l)

    def update(self):
        unprocessed_links = self.frame.get_new(OneSanghuk2Newmanc1CarletozUnProcessedLink)
        if unprocessed_links:
            self.download_links(unprocessed_links)

    def download_links(self, unprocessed_links):
        for link in unprocessed_links:
            print "Got a link to download:", link.full_url
            downloaded = link.download()
            links = extract_next_links(downloaded)
            for l in links:
                if is_valid(l):
                    self.frame.add(Sanghuk2Newmanc1CarletozLink(l))

    def shutdown(self):
        print (
            "Time time spent this session: ",
            time() - self.starttime, " seconds.")
    
def extract_next_links(rawDataObj):
    outputLinks = []
    '''
    rawDataObj is an object of type UrlResponse declared at L20-30
    datamodel/search/server_datamodel.py
    the return of this function should be a list of urls in their absolute form
    Validation of link via is_valid function is done later (see line 42).
    It is not required to remove duplicates that have already been downloaded. 
    The frontier takes care of that.
    
    Suggested library: lxml
    '''
    
    try:
        html_content = html.fromstring(rawDataObj.content)
        urls = html_content.xpath('//a/@href')
        url = re.search('[a-zA-z]*[:]..([a-zA-z0-9]*).([\S]*)',rawDataObj.url)
        if is_valid(rawDataObj.url) == True:
            subdomain_dict[url.group(1)].add(rawDataObj.url)                #url group 1 is subdomain. group 2 is the rest
                                                                            #valid urls are stored into subdomain dict.
        for x in urls:
            outlinks_dict[rawDataObj.url].add(x)                #valid outlinks are stored into outlinks_dict. key is the current page and value is the outlinks

        most_outlinks = -100
        page_name = ""
        for key in outlinks_dict.keys():                        #Find the page with the most outlinks.
            if len(outlinks_dict[key]) > most_outlinks:
                most_outlinks = len(outlinks_dict[key])
                page_name = key
                 
        counter = 0
        for key in subdomain_dict.keys():                       #Count how many valid links have been downloaded.
            counter = counter + len(subdomain_dict[key])        #When counter reaches 3000, the crawler should stop.
        print(counter)

        if counter <= 3000:
            open_file = open("analytics.txt", "w+")
            open_file.write("Subdomains and the number of URLs processed from each subdomain \n")
                
                    
            for key in subdomain_dict.keys():
                open_file.write(str(key) + ".ics.uci.edu:" + "\t" + str(len(subdomain_dict[key])) + "\n")

            open_file.write("The page with the most outlinks: \n")
            open_file.write(str(rawDataObj.url) + "\n")     #str(outlinks_dict[rawDataObj.url]) + "\n")
            open_file.write("The Outlinks are: \n")
            for element in outlinks_dict[rawDataObj.url]:
                open_file.write(str(element) + "\n")
            
            open_file.close()
        
        outputLinks = list(urls)
    except:
        pass
    return outputLinks

def is_valid(url):
    '''
    Function returns True or False based on whether the url has to be
    downloaded or not.
    Robot rules and duplication rules are checked separately.
    This is a great place to filter out crawler traps.
    '''
    parsed = urlparse(url)
    if parsed.scheme not in set(["http", "https"]):
        return False
    try:
        assert(".ics.uci.edu" in parsed.hostname)

        assert(not re.match(".*\.(css|js|bmp|gif|jpe?g|ico|php" + "|png|tiff?|mid|mp2|mp3|mp4"\
            + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
            + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
            + "|thmx|mso|arff|rtf|jar|csv"\
            + "|rm|smil|wmv|swf|wma|zip|rar|gz|pdf)$", parsed.path.lower()))

        assert(len(parsed.path.split("/")) < 11)

        for directory in parsed.path.split("/"):
            assert(len(directory) < 300), "Directory too long: "+directory
            assert(not re.match("calendar", directory, flags = re.IGNORECASE))

        #assert(len(set(parsed.path.split("/"))) == len(parsed.path.split("/"))), "Possible duplicate names in path"

        assert(not re.match(".*\.(css|js|bmp|gif|jpe?g|ico|php" + "|png|tiff?|mid|mp2|mp3|mp4"\
            + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
            + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
            + "|thmx|mso|arff|rtf|jar|csv"\
            + "|rm|smil|wmv|swf|wma|zip|rar|gz|pdf)$", parsed.query.lower()))
        
        return True

    except AssertionError, error:
        return False

    except TypeError:
        return False

