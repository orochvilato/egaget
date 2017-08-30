#!/home/www-data/web2py/applications/obsas/.pyenv/bin/python
# -*- coding: utf-8 -*-

import scrapy
import requests
import json
import re
from scrapy.crawler import CrawlerProcess
import locale
import html2text

locale.setlocale(locale.LC_ALL, 'fr_FR.utf8')

propositions = []
arguments = []
sources = []

def createdir(directory):
    import os
    if not os.path.exists(directory):
        os.makedirs(directory)

class EGASpider(scrapy.Spider):
    name = "sessions"
    base_url = 'https://www.egalimentation.gouv.fr'
    def start_requests(self):
        projets_url = self.base_url+'/api/projects'
        articles_url = self.base_url+'/blog'

        request = scrapy.Request(url=articles_url, callback=self.parse_articles)
        yield request

        request = scrapy.Request(url=projets_url, callback=self.parse_projects)
        yield request




    def parse_articles(self, response):
        for article in response.xpath('//li[contains(@class,"media")]/a/@href'):
            article_url = self.base_url + article.extract()
            request = scrapy.Request(url=article_url, callback=self.parse_article)
            yield request

        suivante = response.xpath('//li[not(contains(@class,"disabled"))]/a[contains(@aria-label,"suivante")]/@href')
        if suivante:
            pagesuivante_url = self.base_url+suivante[0].extract()
            request = scrapy.Request(url=pagesuivante_url, callback=self.parse_articles)
            yield request

    def parse_article(self, response):
        themes = " | ".join([ a.extract() for a in response.xpath('//a[contains(@href,"/themes/")]/text()')])
        consultations = " | ".join([a.extract() for a in response.xpath('//div[@class="block"]/ul/li/a/text()')])
        titre = response.xpath('//div[contains(@class,"container")]/h1/text()')[0].extract()
        img = response.xpath('//div[contains(@class,"container")]/img/@src')[0].extract()
        img_url = self.base_url + img
        img_ext = img[-4:]
        contenu = themes + '\n' + consultations + '\n\n' + html2text.html2text(response.xpath('//div[contains(@class,"container")]/div[@class="block"]')[0].extract())
        r = requests.get(img_url)

        with open('articles/%s%s' % (titre,img_ext),'w') as f:
            f.write(r.content)
        with open('articles/%s.txt' % (titre),'w') as f:
            f.write(contenu.encode('utf8'))

    def parse_projects(self, response):
        jsonresponse = json.loads(response.body_as_unicode())
        for prj in jsonresponse['projects']:
            url = self.base_url + prj['_links']['show']
            request = scrapy.Request(url=url, callback=self.parse_project)
            request.meta['project'] = prj['title']
            request.meta['themes'] = ', '.join([ t['title'] for t in prj['themes']])
            yield request
            #break

    def parse_project(self, response):
        content = response.body_as_unicode()
        consid = re.search(r'"currentProjectStepById":"([^"]+)"',content).groups()[0]
        req = {"operationName":"ConsultationPropositionBoxQuery","query":"query ConsultationPropositionBoxQuery(\n  $consultationId: ID!\n) {\n  consultations(id: $consultationId) {\n    sections {\n      ...SectionRecursiveList_sections\n      id\n    }\n    id\n  }\n}\n\nfragment SectionRecursiveList_sections on Section {\n  ...Section_section\n  sections {\n    ...Section_section\n    sections {\n      ...Section_section\n      sections {\n        ...Section_section\n        sections {\n          ...Section_section\n          id\n        }\n        id\n      }\n      id\n    }\n    id\n  }\n}\n\nfragment Section_section on Section {\n  title\n  slug\n  subtitle\n  contribuable\n  contributionsCount\n  ...OpinionList_section\n}\n\nfragment OpinionList_section on Section {\n  id\n  url\n  slug\n  color\n  contribuable\n  contributionsCount\n  appendixTypes {\n    id\n    title\n    position\n  }\n}\n","variables":{"consultationId":consid}}
        request = scrapy.Request('https://www.egalimentation.gouv.fr/graphql/', method='POST',
                          body=json.dumps(req),
                          headers={'Content-Type':'application/json'},
                          callback=self.parse_sections)
        request.meta['project'] = response.meta['project']
        request.meta['themes'] = response.meta['themes']

        yield request
    def parse_sections(self, response):
        jsonresponse = json.loads(response.body_as_unicode())
        for section in jsonresponse['data']['consultations'][0]['sections']:
            req = {"operationName":"OpinionListQuery","query":"query OpinionListQuery(\n  $sectionId: ID!\n  $limit: Int!\n) {\n  contributionsBySection(sectionId: $sectionId, limit: $limit) {\n    ...Opinion_opinion\n    id\n  }\n}\n\nfragment Opinion_opinion on Opinion {\n  id\n  url\n  title\n  createdAt\n  updatedAt\n  votesCountOk\n  votesCountNok\n  votesCountMitige\n  votesCount\n  versionsCount\n  connectionsCount\n  sourcesCount\n  argumentsCount\n  pinned\n  author {\n    vip\n    displayName\n    media {\n      url\n      id\n    }\n    url\n    id\n  }\n  section {\n    title\n    versionable\n    linkable\n    sourceable\n    voteWidgetType\n    id\n  }\n}\n","variables":{"sectionId":section['id'],"limit":100000}}
            request = scrapy.Request('https://www.egalimentation.gouv.fr/graphql/', method='POST',
                  body=json.dumps(req),
                  headers={'Content-Type':'application/json'},
                  callback=self.parse_section)
            request.meta['project'] = response.meta['project']
            request.meta['themes'] = response.meta['themes']
            request.meta['section'] = section['title']
            yield request
    def parse_section(self, response):
        jsonresponse = json.loads(response.body_as_unicode())
        for contrib in jsonresponse['data']['contributionsBySection']:
            cons = dict(contrib)
            cons['author'] = cons['author']['displayName']
            del cons['url']
            #del cons['id']

            cons.update({u'projet':response.meta['project'],
                         u'themes':response.meta['themes'],
                         u'section':response.meta['section']})

            request = scrapy.Request(url=contrib['url'], callback=self.parse_contribution)
            request.meta['consultation'] = cons

            yield request

    def parse_contribution(self, response):
        content = response.body_as_unicode()
        opinionid = re.search(r'"currentOpinionId":"([^"]+)"',content).groups()[0]
        r = requests.get('https://www.egalimentation.gouv.fr/api/opinions/'+opinionid)
        contribs = r.json()
        cons = response.meta['consultation']
        cons['contenu'] = html2text.html2text(contribs['opinion']['body'])

        propositions.append(cons)
        for src in contribs['opinion']['sources']:
            sources.append({u'projet':cons[u'projet'],
                              u'themes':cons[u'themes'],
                              u'section':cons[u'section'],
                              u'title':cons[u'title'],
                              u'titre_source':src['title'],
                              u'lien':src['link'],
                              u'contenu':html2text.html2text(src['body']),
                              u'author':src['author']['displayName'],
                              u'created_at':src['created_at'],
                              u'updated_at':src['updated_at'],
                              u'id':src['id']})

        for arg in contribs['opinion']['arguments']:
            arguments.append({u'projet':cons[u'projet'],
                              u'themes':cons[u'themes'],
                              u'section':cons[u'section'],
                              u'title':cons[u'title'],
                              u'contenu':html2text.html2text(arg['body']),
                              u'author':arg['author']['displayName'],
                              u'type':'pour' if arg['type']==1 else 'contre',
                              u'created_at':arg['created_at'],
                              u'updated_at':arg['updated_at'],
                              u'id':arg['id']})




process = CrawlerProcess({
    'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)','DOWNLOAD_DELAY':0.02,'CONCURRENT_REQUESTS':32
})

process.crawl(EGASpider)
process.start() # the script will block here until the crawling is finished

sources.sort(key=lambda x:x['id'])
arguments.sort(key=lambda x:x['id'])
propositions.sort(key=lambda x:x['id'])
fields_props = [u'themes',u'projet',u'section',u'title',u'contenu',u'sourcesCount', u'votesCountOk', u'updatedAt', u'connectionsCount', u'createdAt', u'votesCountNok', u'author', u'pinned', u'votesCount', u'argumentsCount', u'versionsCount', u'votesCountMitige',u'id']
fields_args = [u'themes',u'projet',u'section',u'title',u'contenu',u'author',u'type',u'created_at',u'updated_at','id']
fields_srcs = [u'themes',u'projet',u'section',u'title',u'titre_source',u'lien',u'contenu',u'author',u'created_at',u'updated_at','id']


def writexls(name,headers,data):
    from openpyxl import Workbook
    wb = Workbook()
    ws = wb.active
    ws.append(headers)
    for l in data:
        ws.append([l[f] for f in headers])
    wb.save(name)

def createdir(directory):
    import os
    if not os.path.exists(directory):
        os.makedirs(directory)

createdir('csv')
createdir('xlsx')
createdir('articles')
writexls('xlsx/EGA_arguments.xlsx',fields_args,arguments)
writexls('xlsx/EGA_propositions.xlsx',fields_props,propositions)
writexls('xlsx/EGA_sources.xlsx',fields_srcs,sources)

import csv
with open('csv/EGA_arguments.csv','w') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=fields_args, delimiter=' ', quotechar='|')
    writer.writeheader()
    for a in arguments:
        writer.writerow(dict((k,v.encode('utf8') if isinstance(v,basestring) else v) for k,v in a.iteritems()))

with open('csv/EGA_sources.csv','w') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=fields_srcs, delimiter=' ', quotechar='|')
    writer.writeheader()
    for s in sources:
        writer.writerow(dict((k,v.encode('utf8') if isinstance(v,basestring) else v) for k,v in s.iteritems()))


with open('csv/EGA_propositions.csv', 'w') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=fields_props, delimiter=' ', quotechar='|')

    writer.writeheader()
    for c in propositions:
        writer.writerow(dict((k,v.encode('utf8') if isinstance(v,basestring) else v) for k,v in c.iteritems()))
