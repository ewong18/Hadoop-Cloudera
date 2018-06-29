import xml.etree.ElementTree as ET
import sys
import os    
from xml.etree.ElementTree import XML, fromstring, tostring

#set default encoding
reload(sys)
sys.setdefaultencoding('utf8')

#Only for Python 2.7
def itertext(self):
	tag = self.tag
	if not isinstance(tag, str) and tag is not None:
		return
	if self.text:
		yield self.text
	for e in self:
		for s in e.itertext():
			yield s
		if e.tail:
			yield e.tail

xmlpath="<file path>"

x=0
files_in_dir = os.listdir(xmlpath)

for file_in_dir in files_in_dir:
	x+=1
	#for text file of selected manuscript fields from XML
	outfilepath='/data01/home/napapphome/xchng/pmc/tgtout/pmc-manuscript.txt'
	f = open(outfilepath, 'a')

	tree = ET.parse(xmlpath+file_in_dir)
	root = tree.getroot()
	
	#print 'Extracting PMC manuscript fields from XML to text file...'
	for a in root.iter('front'):
		try:
			jtitle=""
			if a.find('.//journal-title') is not None:
				jtitle=''.join(itertext(ET.fromstring(ET.tostring(a.find('.//journal-title')))))
			pmid=""
			if a.find('.//article-id[@pub-id-type="pmid"]') is not None:
				pmid=''.join(itertext(ET.fromstring(ET.tostring(a.find('.//article-id[@pub-id-type="pmid"]')))))
			pmcid=""
			if a.find('.//article-id[@pub-id-type="pmc"]') is not None:
				pmcid=''.join(itertext(ET.fromstring(ET.tostring(a.find('.//article-id[@pub-id-type="pmc"]')))))
			atitle=""
			if a.find('article-meta/title-group/article-title') is not None:
				atitle=''.join(itertext(ET.fromstring(ET.tostring(a.find('article-meta/title-group/article-title')))))
			abstract=""
			if a.find('article-meta/abstract/p') is not None:
				abstract=''.join(itertext(ET.fromstring(ET.tostring(a.find('article-meta/abstract/p')))))
			pubday=""
			if a.find('article-meta/pub-date/day') is not None:
				pubday=''.join(itertext(ET.fromstring(ET.tostring(a.find('article-meta/pub-date/day')))))
			pubmon=""
			if a.find('article-meta/pub-date/month') is not None:
				pubmon=''.join(itertext(ET.fromstring(ET.tostring(a.find('article-meta/pub-date/month')))))
			pubyr=""
			if a.find('article-meta/pub-date/year') is not None:
				pubyr=''.join(itertext(ET.fromstring(ET.tostring(a.find('article-meta/pub-date/year')))))
			pubdate=pubmon+'/'+pubday+'/'+pubyr
		except:
			pass
			
	f.write(pmcid +'|'+pmid+'|'+jtitle+'|'+atitle+'|'+abstract+'|'+pubdate+'\n')
	#print "Closing PMC manuscript text file..."
	f.close()	

	#Get kwd groups
	outfilepath='/data01/home/napapphome/xchng/pmc/tgtout/pmc-manu-kwd-groups.txt'
	f = open(outfilepath, 'a')
	root = tree.getroot()
	#print 'Extracting KWD group fields from XML to text file...'
	for a in root.iter('kwd-group'):
		kwdgrp=""
		try:
			if a.findall('kwd') is not None:
				allkwds = a.findall('kwd')
	
				flg = 0
				while flg < len(allkwds):
					kwdgrp = ''.join(itertext(ET.fromstring(ET.tostring(allkwds[flg]))))
					f.write(pmcid +'|'+pmid+'|'+kwdgrp+'\n')
					flg+=1		
		except:
			pass

	#print "Closing KWD group text file..."
	f.close()		
			
	#for text file of selected Author tags from XML
	outfilepath='/data01/home/napapphome/xchng/pmc/tgtout/pmc-manu-authors.txt'
	f = open(outfilepath, 'a')

	root = tree.getroot()
	#print 'Extracting author fields from XML to text file...'
	for a in root.iter('contrib'):
		try:
			auth_lst_nm=""
			if a.find('name/surname') is not None:
				auth_lst_nm=''.join(itertext(ET.fromstring(ET.tostring(a.find('name/surname')))))
			auth_fn_mi=""
			if a.find('name/given-names') is not None:
				auth_fn_mi=''.join(itertext(ET.fromstring(ET.tostring(a.find('name/given-names')))))
			auth_affil_addr=""
			if a.find('xref') is not None:
				auth_affil_grp_id = a.find('xref').get('rid')
				if (a.find('xref').get('ref-type') == 'aff'):
					aff_grp_id = root.find('.//aff[@id="'+auth_affil_grp_id+'"]')		
					auth_affil_addr = aff_grp_id.find('label').tail
				#not working properly
				elif (a.find('xref').get('ref-type') == 'author-notes'):
					auth_affil_addr = ''.join(itertext(ET.fromstring(ET.tostring(root.find('.//corresp[@id="'+auth_affil_grp_id+'"]')))))
					#auth_affil_addr = root.find('.//corresp[@id="'+auth_affil_grp_id+'"]').tail
			elif a.find('xref') is None:
				auth_affil_addr = ''.join(itertext(ET.fromstring(ET.tostring(a.find('aff')))))
		except:
			pass
		
		#Work around. Sometimes auth_affil_addr ends up as a NoneType
		if auth_affil_addr is not None:	
			f.write(pmcid +'|'+pmid+'|'+auth_lst_nm+'|'+auth_fn_mi+'|'+auth_affil_addr +'\n')
		else:
			f.write(pmcid +'|'+pmid+'|'+auth_lst_nm+'|'+auth_fn_mi+'|'+ '\n')
	#print "Closing PMC manuscript authors text file..."
	f.close()		
			
	#for text file of selected Citation articles tags from XML
	outfilepath='/data01/home/napapphome/xchng/pmc/tgtout/pmc-manu-citation-articles.txt'
	outfilepath2='/data01/home/napapphome/xchng/pmc/tgtout/pmc-manu-citation-authors.txt'
	f = open(outfilepath, 'a')
	g = open(outfilepath2, 'a')

	root = tree.getroot()
	#print 'Extracting citation article and author fields from XML to separate text files...'
	for a in root.iter('ref'):
	
		try:
			cit_title_yr=""
			if a.find('.//year') is not None:
				cit_title_yr=''.join(itertext(ET.fromstring(ET.tostring(a.find('.//year')))))
			cit_title=""
			if a.find('.//article-title') is not None:
				cit_title=''.join(itertext(ET.fromstring(ET.tostring(a.find('.//article-title')))))
			cit_journal=""
			if a.find('.//source') is not None:
				cit_journal=''.join(itertext(ET.fromstring(ET.tostring(a.find('.//source')))))
			cit_journal_vol=""
			if a.find('.//volume') is not None:
				cit_journal_vol=''.join(itertext(ET.fromstring(ET.tostring(a.find('.//volume')))))
			cit_pmid=""
			if a.find('.//pub-id') is not None:
				cit_pmid=''.join(itertext(ET.fromstring(ET.tostring(a.find('.//pub-id')))))
				
			f.write(pmcid +'|'+pmid+'|'+cit_pmid+'|'+cit_journal+'|'+cit_journal_vol+'|'+cit_title+'|'+cit_title_yr+'\n')
		except:
			pass

		for b in a.iter('person-group'):
			try:
				cit_auth_ln=""
				if b.findall('name/surname') is not None:
					cit_auth_ln=b.findall('name/surname')
				cit_auth_fn_mi=""
				if b.findall('name/given-names') is not None:
					cit_auth_fn_mi=b.findall('name/given-names')
					
				flag = 0
				while flag < len(cit_auth_ln):
					g.write(pmcid +'|'+pmid+'|'+cit_pmid+'|'+cit_auth_ln[flag].text+'|'+cit_auth_fn_mi[flag].text+'\n')
					flag+=1			
			except:
				pass
	#print 'Closing citation article and author text files...'		
	f.close()
	g.close()
print 'Files read: ' + str(x)
print 'Directory read: ' + xmlpath
