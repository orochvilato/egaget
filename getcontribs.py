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

consultations = []
arguments = []
sources = []

class EGASpider(scrapy.Spider):
    name = "sessions"
    base_url = 'https://www.egalimentation.gouv.fr'
    start_url = base_url+'/api/projects'
    def start_requests(self):
        urls = [ self.start_url]

        for url in urls:
            request = scrapy.Request(url=url, callback=self.parse_projects)
            yield request

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

        consultations.append(cons)
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
fields = [u'themes',u'projet',u'section',u'title',u'contenu',u'sourcesCount', u'votesCountOk', u'updatedAt', u'connectionsCount', u'createdAt', u'votesCountNok', u'author', u'pinned', u'votesCount', u'argumentsCount', u'versionsCount', u'votesCountMitige',u'id']
fields_args = [u'themes',u'projet',u'section',u'title',u'contenu',u'author',u'type',u'created_at',u'updated_at','id']
sources_args = [u'themes',u'projet',u'section',u'title',u'titre_source',u'lien',u'contenu',u'author',u'created_at',u'updated_at','id']
import csv
with open('EGA_arguments.csv','w') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=fields_args)
    writer.writeheader()
    for a in arguments:
        writer.writerow(dict((k,v.encode('utf8') if isinstance(v,basestring) else v) for k,v in a.iteritems()))

with open('EGA_sources.csv','w') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=sources_args)
    writer.writeheader()
    for s in sources:
        writer.writerow(dict((k,v.encode('utf8') if isinstance(v,basestring) else v) for k,v in s.iteritems()))


with open('EGA_propositions.csv', 'w') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=fields)

    writer.writeheader()
    for c in consultations:
        writer.writerow(dict((k,v.encode('utf8') if isinstance(v,basestring) else v) for k,v in c.iteritems()))
