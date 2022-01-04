<p>The revamped OMC to BDA tables keep track of what elements members clicked on inside each email.</p><p>Every single clicked element is stored in a field called <i>clicked_elements</i>&nbsp;in sorted order and separated by commas.</p><p>This snippet is going to take all the emails we've sent and explode them so that each email produces 1 row per clicked element holding only that clicked element. Emails where the member hasn't clicked any element generate a single row with a blank <i>clicked_elements</i> field.</p><p>By selecting the distinct <i>clicked_elements </i>from this exploded view we can automatically produce code that tests whether a user has clicked each of these elements (the <i>binarization logic</i>).</p> 
import pyspark.sql.functions as f 
 
# Pulling in data for the campaign we want to examine 
exploded = spark.sql(''' 
SELECT 
    customer_id 
  , segment 
  , skip_reason IS NOT NULL AS skip 
  , bounce_event_date IS NOT NULL AS bounce 
  , open_event_date 
  , click_event_date 
  , unsubbed_event_date 
  -- Coalesce or NVL required as the explode function we'll be using will eliminate any rows where clicked_elements is NULL 
  , COALESCE(clicked_elements, '') AS clicked_elements 
 
FROM 
    omc.send_level_summary 
     
WHERE 
    campaign_id = '47245422' 
''') 
 
# Transforms the data from having one entry per member to having one entry per member-clicked element pair 
# Split turns the contents of a field into a list using the given delimiter 
# For instance, the string "A B C" split by " " becomes the list of strings ["A", "B", "C"] 
# Exploded takes lists and create individual rows out of them 
# For instance, if we explode on clicked_elements and a row has a clicked_elements value of ["A", "B", "C"] 
# we get three copies of that row, one where clicked_elements is "A", one where it is "B", and one where it is "C" 
# Note that if the value was instead [], the row would be deleted as it creates rows equal to the length of the list (which would be zero) 
exploded.withColumn('clicked_elements', f.explode(f.split('clicked_elements', ','))).createOrReplaceTempView('exploded') 
 
# Grabs all the unique clicked elements and dumps them into a python local variable 
elements = spark.sql(''' 
SELECT DISTINCT 
    clicked_elements 
     
FROM 
    exploded 
''').collect() 
 
# Creates a the binarization logic 
# Basically the template tests whether a given element was clicked, returning True if it was 
# List comprehension is used to apply it to every possible element 
# ALso ignores the empty string (the result of our earlier COALESCE) from the list 
template = "  , COALESCE(MAX(clicked_elements = '{el}'), FALSE) AS {el}" 
element_binarizer = '\n'.join([template.format(el = e['clicked_elements']) for e in elements if e['clicked_elements'] != '']) 
print element_binarizer 
<p>Saves the binarization logic. This is basically the code spat out from the first snippet but it's been modified to give everything more meaningful names and to group together a few clicked_elements.</p> 
element_binarizer = ''' 
  , COALESCE(MAX(clicked_elements LIKE 'Check_it_now_covered_for_roadside%'), FALSE) AS ns_coverage 
  , COALESCE(MAX(clicked_elements = 'img_Road_trip_tips'), FALSE) AS c_roadtriptips 
  , COALESCE(MAX(clicked_elements = 'Contact_us'), FALSE) AS s_contactus 
  , COALESCE(MAX(clicked_elements = 'Youtube'), FALSE) AS sm_youtube 
  , COALESCE(MAX(clicked_elements = 'View_Onine'), FALSE) AS s_viewonline 
  , COALESCE(MAX(clicked_elements = 'NRMA_APP'), FALSE) AS app_app 
  , COALESCE(MAX(clicked_elements = 'Privacy_Policy'), FALSE) AS s_privacy 
  , COALESCE(MAX(clicked_elements = 'NRMA_Roadside_assistance'), FALSE) AS s_rsa 
  , COALESCE(MAX(clicked_elements = 'General_Conditions'), FALSE) AS s_conditions 
  , COALESCE(MAX(clicked_elements = 'img_For_all_journeys_great_and_small'), FALSE) AS c_alljourneys 
  , COALESCE(MAX(clicked_elements = 'img_Hotels_wherever_youre_headed'), FALSE) AS o_hotels 
  , COALESCE(MAX(clicked_elements = 'Facebook'), FALSE) AS sm_facebook 
  , COALESCE(MAX(clicked_elements LIKE '%_Go_somewhere_great'), FALSE) AS o_parksresorts 
  , COALESCE(MAX(clicked_elements = 'NRMA_MEMBERSHIP'), FALSE) AS s_membership 
  , COALESCE(MAX(clicked_elements = 'Instagram'), FALSE) AS sm_instagram 
  , COALESCE(MAX(clicked_elements = 'img_Fill_up_and_save'), FALSE) AS o_caltex 
  , COALESCE(MAX(clicked_elements = 'Take_a_Look_CTA'), FALSE) AS c_cta 
  , COALESCE(MAX(clicked_elements = 'img_Explore_new_experiences'), FALSE) AS o_experiences 
  , COALESCE(MAX(clicked_elements = 'img_Where_to_next'), FALSE) AS c_whereto 
  , COALESCE(MAX(clicked_elements = 'Twitter'), FALSE) AS sm_twitter 
''' 
# Applies the binarization logic 
spark.sql(''' 
SELECT 
    customer_id 
  , segment 
  , MIN(CASE WHEN skip OR bounce THEN FALSE ELSE TRUE END) AS delivered 
  , MIN(COALESCE(open_event_date, click_event_date)) AS open 
  , MIN(click_event_date) AS click 
  , MIN(unsubbed_event_date) AS unsub 
{bin} 
 
FROM 
    exploded 
     
GROUP BY 
    1 
  , 2 
'''.format(bin = element_binarizer)).show(250, False) 