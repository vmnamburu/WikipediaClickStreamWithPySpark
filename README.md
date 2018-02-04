
This note is to demonstrate how to analyse clickstream data from Wikipedia using pyspark. This example covers analysis of Feb 2015 clickstream file.

__Tools Used__

1. Cloudera Quickstart VM 5.10
2. Jupyter Notebook

## 1. Understand File structure of the Clickstream file

#### 1.1 Download Feb 2015 clickstream file

https://ndownloader.figshare.com/files/5036383


```python
# Download file as 2015_02_en_clickstream.tsv
$wget https://ndownloader.figshare.com/files/5036383 --output-document=2015_02_en_clickstream.tsv

# Move file to HDFS
$hadoop fs -put 2015_02_en_clickstream.tsv /user/cloudera/wikiClickstream/
```

#### 1.2 Examine the content and structure of this file


```python
[cloudera@quickstart wikiClickStream]$ head 2015_02_en_clickstream.tsv
prev_id curr_id n       prev_title      curr_title      type
        3632887 121     other-google    !!      other
        3632887 93      other-wikipedia !!      other
        3632887 46      other-empty     !!      other
        3632887 10      other-other     !!      other
64486   3632887 11      !_(disambiguation)      !!      other
2061699 2556962 19      Louden_Up_Now   !!!_(album)     link

```

The file contains information of the requestor and resource pairs with total number of visits
Each line has 

__prev_id :__

The wikipedia page ID from which the user has requested another wikipedia page. A non-empty number indicates n users have visited the page (curr_id,curr_title) page from the page (prev_id,prev_title)

__curr_id :__

The wikipedia page ID to which the user has navigated from another page

__n:__

Total number of requests for the combination of (prev_id,prev_title) to (curr_id,curr_title)


__prev_title:__

The title of the wikipediae page or an external source . 
Titles like other-google, other-bing indicate, the request came from external sources to wikipedia


__curr_title:__

 The title of the requested wikipedia page

__type:__

Type of link from the source to reqeuest page

#### 1.3 Example lines from the file for WikiPage of Lucasfilm


```python
80872   28932764        72      Lucasfilm       Star_Tours—The_Adventures_Continue      link
80872   10269131        75      Lucasfilm       Star_Wars:_The_Clone_Wars_(2008_TV_series)      link
80872   14723194        1096    Lucasfilm       Star_Wars:_The_Force_Awakens    link
```

These 3 lines indicate
1. __80872__ is the ID of the page [__Lucasfilm__](https://en.wikipedia.org/wiki/Lucasfilm)
1. __72__ requests from __Lucasfilm__ page to the page [__Star_Tours—The_Adventures_Continue__](https://en.wikipedia.org/wiki/Star_Tours_%E2%80%93_The_Adventures_Continue), whose ID is 28932764
2. __75__ requests from __Lucasfilm__ page to the page [__Star_Wars:_The_Clone_Wars_(2008_TV_series)__](https://en.wikipedia.org/wiki/Star_Wars:_The_Clone_Wars_(2008_TV_series), whose ID is 10269131
3. __1096__ requests from __Lucasfilm__ page to the page [__Star_Wars:_The_Force_Awakens__](https://en.wikipedia.org/wiki/Star_Wars%3A_The_Force_Awakens), whose ID is 14723194

#### 1.4 Example lines for Star Wars Episode II resulting from a Google Search


```python
        4936424 48      other-google    Star_Wars_Episode_II:_Attack_of_the_Clones_(novel)      other
        4398290 459     other-google    Star_Wars_Episode_II:_Attack_of_the_Clones_(soundtrack) other
        4734835 95      other-google    Star_Wars_Episode_II:_Attack_of_the_Clones_(video_game) other
```

#### 1.5 Examples lines for Star Wars directed from Social Networks (Twitter)


```python
        10269131        47      other-twitter   Star_Wars:_The_Clone_Wars_(2008_TV_series)      other
        2885266 150     other-twitter   Star_Wars:_The_Empire_Strikes_Back_(1985_video_game)    other
```

### 2. Convert the File to a Spark Dataframe to perform analysis


```python
# Import pyspark libraries
from pyspark.sql import Row,SQLContext
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType
```


```python
#Create SQLContext
sqlCtx = SQLContext(sc)
```

#### 2.1 Create a function to convert empty strings to a -ve numeric value. int() on empty string fails


```python
def getInt(field):
    if field == '':
        return -1
    else:
        return int(field)
```

#### 2.2 Create a functiont to convert each line to a Row object


```python
def convertToRow(line):
    prev_id,curr_id,cnt,prev_title,curr_title,type=line.split('\t')
    return Row(prev_id=getInt(prev_id),curr_id=getInt(curr_id),n=int(cnt),prev_title=prev_title,curr_title=curr_title,type=type)
```

#### 2.3 Load the clickstream file to RDD


```python
clickRDD = sc.textFile('/user/cloudera/wikiClickstream/2015_02_en_clickstream.tsv').map(convertToRow)
clickRDD.take(2)
```




    [Row(curr_id=3632887, curr_title=u'!!', n=121, prev_id=-1, prev_title=u'other-google', type=u'other'),
     Row(curr_id=3632887, curr_title=u'!!', n=93, prev_id=-1, prev_title=u'other-wikipedia', type=u'other')]



#### 2.4 Create a Dataframe from the RDD


```python
clickDF = sqlCtx.createDataFrame(clickRDD)
clickDF.show()
```

    +--------+-------------+---+--------+--------------------+-----+
    | curr_id|   curr_title|  n| prev_id|          prev_title| type|
    +--------+-------------+---+--------+--------------------+-----+
    | 3632887|           !!|121|      -1|        other-google|other|
    | 3632887|           !!| 93|      -1|     other-wikipedia|other|
    | 3632887|           !!| 46|      -1|         other-empty|other|
    | 3632887|           !!| 10|      -1|         other-other|other|
    | 3632887|           !!| 11|   64486|  !_(disambiguation)|other|
    | 2556962|  !!!_(album)| 19| 2061699|       Louden_Up_Now| link|
    | 2556962|  !!!_(album)| 25|      -1|         other-empty|other|
    | 2556962|  !!!_(album)| 16|      -1|        other-google|other|
    | 2556962|  !!!_(album)| 44|      -1|     other-wikipedia|other|
    | 2556962|  !!!_(album)| 15|   64486|  !_(disambiguation)| link|
    | 2556962|  !!!_(album)|297|  600744|                 !!!| link|
    | 6893310|!Hero_(album)| 11|      -1|         other-empty|other|
    | 6893310|!Hero_(album)| 26| 1921683|               !Hero| link|
    | 6893310|!Hero_(album)| 16|      -1|     other-wikipedia|other|
    | 6893310|!Hero_(album)| 23|      -1|        other-google|other|
    |22602473|   !Oka_Tokat| 16| 8127304|     Jericho_Rosales| link|
    |22602473|   !Oka_Tokat| 20|35978874|List_of_telenovel...| link|
    |22602473|   !Oka_Tokat| 57|      -1|        other-google|other|
    |22602473|   !Oka_Tokat| 12|      -1|     other-wikipedia|other|
    |22602473|   !Oka_Tokat| 23|      -1|         other-empty|other|
    +--------+-------------+---+--------+--------------------+-----+
    only showing top 20 rows
    


### 3 Analyse what are the top searches leading to Wikipedia in Feb 2015

### 3.1 Analyse the Clickstream using Spark Dataframes

##### 3.1.1 Create a function to apply as a filter


```python
def fromSearchProvider(prevTitle):
    searchProviders = ['other-yahoo','other-bing','other-google']
    if prevTitle in searchProviders:
        return True
    else:
        return False
```

##### 3.1.2 Create an UDF to supply fromSearchProvider as filter function


```python
search_filter = udf(fromSearchProvider,BooleanType())
clickSearch = clickDF.filter(search_filter(clickDF.prev_title))
clickSearch.show()
```

    +--------+--------------------+----+-------+------------+-----+
    | curr_id|          curr_title|   n|prev_id|  prev_title| type|
    +--------+--------------------+----+-------+------------+-----+
    | 3632887|                  !!| 121|     -1|other-google|other|
    | 2556962|         !!!_(album)|  16|     -1|other-google|other|
    | 6893310|       !Hero_(album)|  23|     -1|other-google|other|
    |22602473|          !Oka_Tokat|  57|     -1|other-google|other|
    | 6810768|          !T.O.O.H.!|  81|     -1|other-google|other|
    |  899480|          "A"_Device|  17|     -1|other-google|other|
    | 1282996|    "A"_Is_for_Alibi|  10|     -1| other-yahoo|other|
    | 1282996|    "A"_Is_for_Alibi| 272|     -1|other-google|other|
    | 9003666|"And"_theory_of_c...|  18|     -1|other-google|other|
    |39072529|"Bassy"_Bob_Brock...|  49|     -1|other-google|other|
    |25033979|"C"_is_for_(Pleas...|  93|     -1|other-google|other|
    |  331586|  "Crocodile"_Dundee|6820|     -1|other-google|other|
    |  331586|  "Crocodile"_Dundee| 274|     -1| other-yahoo|other|
    |  331586|  "Crocodile"_Dundee| 417|     -1|  other-bing|other|
    |16250593| "D"_Is_for_Deadbeat|  21|     -1|other-google|other|
    |39304968|"David_Hockney:_A...| 108|     -1|other-google|other|
    | 1896643|"Dr._Death"_Steve...|1227|     -1|other-google|other|
    | 1896643|"Dr._Death"_Steve...|  70|     -1| other-yahoo|other|
    | 1896643|"Dr._Death"_Steve...|  75|     -1|  other-bing|other|
    |16251903| "E"_Is_for_Evidence|  26|     -1|other-google|other|
    +--------+--------------------+----+-------+------------+-----+
    only showing top 20 rows
    


##### 3.1.3 Find the top pages referred from search Engines


```python
searchVolume = clickSearch.groupBy(clickSearch.curr_title).agg(F.sum('n').alias('total_search')).orderBy('total_search',ascending=False)
searchVolume.show()
```

    +--------------------+------------+
    |          curr_title|total_search|
    +--------------------+------------+
    |           Main_Page|     4171329|
    |Fifty_Shades_of_Grey|     1903372|
    |          Chris_Kyle|     1293055|
    |    Alessandro_Volta|     1160284|
    |     Stephen_Hawking|     1037257|
    |    Better_Call_Saul|      989149|
    |      Birdman_(film)|      982244|
    |Fifty_Shades_of_G...|      877027|
    |     Valentine's_Day|      831627|
    | 87th_Academy_Awards|      794562|
    |Islamic_State_of_...|      775541|
    |    Chinese_New_Year|      740223|
    |       Leonard_Nimoy|      683814|
    |List_of_Bollywood...|      653926|
    |        Bruce_Jenner|      629555|
    |      Sia_(musician)|      618234|
    |      Lunar_New_Year|      602595|
    |      Dakota_Johnson|      598695|
    |The_Walking_Dead_...|      581170|
    |The_Flash_(2014_T...|      558487|
    +--------------------+------------+
    only showing top 20 rows
    


####  3.2 Analysis using Spark SQL

#### 3.2.1 Register DataFrame as a table


```python
clickDF.registerTempTable('wikiclickstream')
```


```python
clickSearchSQL = sqlCtx.sql("select curr_title,sum(n) as total_search from wikiclickstream where prev_title in ('other-yahoo','other-bing','other-google') group by curr_title order by total_search desc")
clickSearchSQL.show()
```

    +--------------------+------------+
    |          curr_title|total_search|
    +--------------------+------------+
    |           Main_Page|     4171329|
    |Fifty_Shades_of_Grey|     1903372|
    |          Chris_Kyle|     1293055|
    |    Alessandro_Volta|     1160284|
    |     Stephen_Hawking|     1037257|
    |    Better_Call_Saul|      989149|
    |      Birdman_(film)|      982244|
    |Fifty_Shades_of_G...|      877027|
    |     Valentine's_Day|      831627|
    | 87th_Academy_Awards|      794562|
    |Islamic_State_of_...|      775541|
    |    Chinese_New_Year|      740223|
    |       Leonard_Nimoy|      683814|
    |List_of_Bollywood...|      653926|
    |        Bruce_Jenner|      629555|
    |      Sia_(musician)|      618234|
    |      Lunar_New_Year|      602595|
    |      Dakota_Johnson|      598695|
    |The_Walking_Dead_...|      581170|
    |The_Flash_(2014_T...|      558487|
    +--------------------+------------+
    only showing top 20 rows
    


Why are these the top topics searched in Feb 2015 

__Movies__
1. __Fifty Shades of Grey__ released in Feb 2015 featuring __Dakota Johnson__
2. __Birdman__ won 4 awards in __87th Academy Awards__

__People__
1. __Alessandro Volta__ is an Italian Physicist born on 14 Feb 1745. Google published a [Doodle](https://www.theguardian.com/science/the-h-word/2015/feb/18/alessandro-volta-anniversary-electricity-history-science), the potential reason for being one of the most searched Person in Feb 2015
2. __Chris Kyle__ a US Navy SEAL and sniper died in Feb
3. __Stephen Hawking__ attended an awards function for his biopic *Theory of Everything*
4. __Leonard_Nimoy__, the *Spock* of *Star Trek* died on 27 Feb 2015

__Events or Occassions__
1. __Chinese New year__ or __Lunar New Year__ is celebrated on 19 February 2015
2. __Valentines Day__ is on 14th February

###### *Note: Spark SQL code came out simple and elegant compared to DataFrame*

### 4 Analyse what are the top searches from Social Networks leading to Wikipedia in Feb 2015

#### 4.1 Analyse the Clickstream using Spark Dataframes

##### 4.1.1 Create a function to apply as a filter for social networks


```python
def fromSocialNetwork(prevTitle):
    searchProviders = ['other-twitter','other-facebook']
    if prevTitle in searchProviders:
        return True
    else:
        return False
```

##### 4.1.2 Create an UDF to supply fromSocialNetwork as filter function


```python
social_filter = udf(fromSocialNetwork,BooleanType())
clickSocial = clickDF.filter(social_filter(clickDF.prev_title))
clickSocial.show()
```

    +--------+--------------------+---+-------+--------------+-----+
    | curr_id|          curr_title|  n|prev_id|    prev_title| type|
    +--------+--------------------+---+-------+--------------+-----+
    |  331586|  "Crocodile"_Dundee| 20|     -1| other-twitter|other|
    | 1261557|            "Heroes"| 13|     -1| other-twitter|other|
    | 3564374|     "I_AM"_Activity| 33|     -1|other-facebook|other|
    | 3564374|     "I_AM"_Activity| 24|     -1| other-twitter|other|
    |18938265| "Weird_Al"_Yankovic|406|     -1| other-twitter|other|
    |18938265| "Weird_Al"_Yankovic| 33|     -1|other-facebook|other|
    | 7630017|"Weird_Al"_Yankov...| 67|     -1| other-twitter|other|
    | 1578140|                  %s| 13|     -1| other-twitter|other|
    |    2676|    'Abd_al-Rahman_I| 16|     -1| other-twitter|other|
    |  430164|        'Allo_'Allo!| 13|     -1|other-facebook|other|
    |  430164|        'Allo_'Allo!| 67|     -1| other-twitter|other|
    |  175149|        'Pataphysics| 36|     -1|other-facebook|other|
    |  175149|        'Pataphysics| 96|     -1| other-twitter|other|
    | 1917971|                  's| 12|     -1| other-twitter|other|
    |   50338|    's-Hertogenbosch| 16|     -1|other-facebook|other|
    |   50338|    's-Hertogenbosch| 10|     -1| other-twitter|other|
    |42995159|  (357439)_2004_BL86| 30|     -1|other-facebook|other|
    |42995159|  (357439)_2004_BL86| 24|     -1| other-twitter|other|
    | 1506853|(Don't_Fear)_The_...|172|     -1| other-twitter|other|
    | 2448083|(Everything_I_Do)...| 16|     -1| other-twitter|other|
    +--------+--------------------+---+-------+--------------+-----+
    only showing top 20 rows
    


##### 4.1.3 Find the top pages referred from social network sites


```python
socialVolume = clickSocial.groupBy(clickSearch.curr_title).agg(F.sum('n').alias('total_social')).orderBy('total_social',ascending=False)
socialVolume.show()
```

    +--------------------+------------+
    |          curr_title|total_social|
    +--------------------+------------+
    |    Johnny_Knoxville|      198976|
    |      Peter_Woodcock|      126378|
    |2002_Tampa_plane_...|      120955|
    |      Sơn_Đoòng_Cave|      116126|
    |       The_boy_Jones|      114524|
    |             War_pig|      114138|
    |William_Leonard_P...|      113906|
    |Hurt_(Nine_Inch_N...|      103562|
    |     Glass_recycling|       87995|
    |Assassination_of_...|       86445|
    |    Fury_(2014_film)|       80297|
    |    Mullet_(haircut)|       73613|
    |            Iron_Man|       69772|
    |International_Mat...|       64475|
    |Pirates_of_the_Ca...|       63517|
    |            Asbestos|       62987|
    |       Benjaman_Kyle|       61130|
    |            New_Deal|       59854|
    |     Bobbie_Joe_Long|       59836|
    |        David_Reimer|       59136|
    +--------------------+------------+
    only showing top 20 rows
    


####  4.2 Analysis using Spark SQL


```python
clickSocialSQL = sqlCtx.sql("select curr_title,sum(n) as total_search from wikiclickstream where prev_title in ('other-twitter','other-facebook') group by curr_title order by total_search desc")
clickSocialSQL.show()
```

    +--------------------+------------+
    |          curr_title|total_search|
    +--------------------+------------+
    |    Johnny_Knoxville|      198976|
    |      Peter_Woodcock|      126378|
    |2002_Tampa_plane_...|      120955|
    |      Sơn_Đoòng_Cave|      116126|
    |       The_boy_Jones|      114524|
    |             War_pig|      114138|
    |William_Leonard_P...|      113906|
    |Hurt_(Nine_Inch_N...|      103562|
    |     Glass_recycling|       87995|
    |Assassination_of_...|       86445|
    |    Fury_(2014_film)|       80297|
    |    Mullet_(haircut)|       73613|
    |            Iron_Man|       69772|
    |International_Mat...|       64475|
    |Pirates_of_the_Ca...|       63517|
    |            Asbestos|       62987|
    |       Benjaman_Kyle|       61130|
    |            New_Deal|       59854|
    |     Bobbie_Joe_Long|       59836|
    |        David_Reimer|       59136|
    +--------------------+------------+
    only showing top 20 rows
    


### 5 Which referred pages lead to maximum broken links

A Broken link is identified with a -1 in curr_id and the type has a value of redlink


```python
brokenLinks = clickDF.filter(clickDF.curr_id == -1)
brokenLinks.show()
```

    +-------+--------------------+---+--------+--------------------+-------+
    |curr_id|          curr_title|  n| prev_id|          prev_title|   type|
    +-------+--------------------+---+--------+--------------------+-------+
    |     -1|   "Bigfoot"_Wallace| 15|11273993|Colt_1851_Navy_Re...|redlink|
    |     -1|"Chúc_Mừng_Năm_Mớ...| 51|   69161|                 Tết|redlink|
    |     -1|"Cool_Hand_Conor"...| 14| 1438509|List_of_Old_West_...|redlink|
    |     -1|"D"_Is_for_Dubby_...| 47| 4619790|            Puscifer|redlink|
    |     -1|"D"_Is_for_Dubby_...| 43|16079543|"V"_Is_for_Viagra...|redlink|
    |     -1|"D"_Is_for_Dubby_...| 18|25033979|"C"_is_for_(Pleas...|redlink|
    |     -1|"EXO_Music_Video_...| 23|39737124|         Yoon_So-hee|redlink|
    |     -1|"Firth"_logistic_...| 24|29668256|Separation_(stati...|redlink|
    |     -1|"Future_(rapper)"...| 20| 1696824|   Kirkwood,_Atlanta|redlink|
    |     -1|     "Knockin'_Boots| 10| 2110406|        Pretty_Ricky|redlink|
    |     -1| "Mothercare"_spider| 20| 3419979|    Woodlouse_spider|redlink|
    |     -1|"One_Arm_Bill"_Wi...| 13|25133882|Goodnight–Loving_...|redlink|
    |     -1|          "That_Man"| 14|24961418|        Caro_Emerald|redlink|
    |     -1|"The_Mills",_Andr...| 12| 1442421|Chadds_Ford_Towns...|redlink|
    |     -1| "Tinker_Dave"_Beaty| 24| 2804228|      Champ_Ferguson|redlink|
    |     -1|         "et_cetera"|115|  202033|                 ECT|redlink|
    |     -1|              "etc."|244|  202033|                 ECT|redlink|
    |     -1| '''Akhand_Bharat'''| 37|30863671|List_of_newspaper...|redlink|
    |     -1| '''Dainik_Jagran'''| 84|30863671|List_of_newspaper...|redlink|
    |     -1|'''Luppak_state''...| 10|18934934|    Read-only_memory|redlink|
    +-------+--------------------+---+--------+--------------------+-------+
    only showing top 20 rows
    



```python
clickBrokenLinks = clickDF.filter(clickDF.curr_id == -1).groupBy('prev_title').agg(F.sum(clickDF.n).alias('total_broken')).orderBy('total_broken',ascending=False)
clickBrokenLinks.show()
```

    +--------------------+------------+
    |          prev_title|total_broken|
    +--------------------+------------+
    |List_of_adult_tel...|       16812|
    |      Deaths_in_2015|       10184|
    |  Illusion_(company)|        9552|
    |List_of_festivals...|        7673|
    |2023_Cricket_Worl...|        7447|
    |         Tom_Selleck|        6434|
    |           Sinusitis|        5715|
    |  Outline_of_thought|        5314|
    |Meri_Aashiqui_Tum...|        4989|
    |List_of_TVB_drama...|        4739|
    |List_of_UPnP_AV_m...|        4391|
    |List_of_oil_refin...|        4313|
    |List_of_Tamil_fil...|        4287|
    |The_Bachelor_(U.S...|        3927|
    |Saath_Nibhaana_Sa...|        3875|
    |        Pawan_Kalyan|        3764|
    |Nisha_Aur_Uske_Co...|        3661|
    |    2014_Mr._Olympia|        3461|
    |        Marc_Anthony|        3408|
    |List_of_Victoria'...|        3284|
    +--------------------+------------+
    only showing top 20 rows
    



```python

```
