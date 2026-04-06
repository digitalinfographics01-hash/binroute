"""
BinRoute AI — Score Pending Orders

Takes the 3rd-party subscription tool export, matches each order to our DB
(via Parent Order ID → customer → BIN data), and scores every available
gateway to find the optimal routing.

For already-processed orders: validates AI prediction vs actual outcome.
For queued orders: recommends the best gateway.

Usage: py -3 scripts/ml/score_pending_orders.py
"""

import os
import sys
import sqlite3
import numpy as np
import pandas as pd
from sklearn.preprocessing import LabelEncoder
import onnxruntime as ort
import warnings
warnings.filterwarnings('ignore')

DB_PATH = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'binroute.db')
MODEL_PATH = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'models', 'binroute_lightgbm.onnx')

CATEGORICAL_FEATURES = [
    'processor_name', 'acquiring_bank', 'mcc_code',
    'issuer_bank', 'card_brand', 'card_type',
    'tx_class', 'cycle_depth', 'prev_decline_reason',
    'initial_processor',
]
NUMERICAL_FEATURES = [
    'is_prepaid', 'amount', 'attempt_number',
    'hour_of_day', 'day_of_week',
    'mid_velocity_daily', 'mid_velocity_weekly',
    'customer_history_on_proc', 'bin_velocity_weekly',
    'consecutive_approvals', 'days_since_last_charge',
    'days_since_initial', 'lifetime_charges', 'lifetime_revenue',
    'initial_amount', 'amount_ratio', 'prior_declines_in_cycle',
]

# ---------------------------------------------------------------------------
# Parse the pasted data
# ---------------------------------------------------------------------------

ORDERS_RAW = """341819	644455	2	273	Yes	Approved	1st Decline Recycling	--	189	JoyP_PNC_0920_30K_(189)	187	JoyP_PNC_0946_30K_(187)	--	647984	4/4/2026 14:49	79.97	Stephen Mooney
341507	644144	1	267	Yes	Approved	2nd Decline Recycling	--	190	JoyP_PNC_0953_30K_(190)	194	Ridge_SYN(PPS)_1843_50K_(194)	191	647946	4/4/2026 12:13	59.97	Alan Hochderffer
341489	640110	1	282	Yes	Approved	2nd Decline Recycling	--	191	JoyP_PNC_0938_30K_(191)	188	JoyP_PNC_0961_30K_(188)	--	647923	4/4/2026 10:27	39.97	Woodrow Morris III
341305	640511	1	267	No	Approved	3rd Decline Recycling	--	189	JoyP_PNC_0920_30K_(189)	172	JoyP_Cliq(Avidia)_0687_20K_(172)	--	647953	4/4/2026 12:48	44.91	Rony jean noel
341265	646639	1	267	No	Approved	Linked	--	187	JoyP_PNC_0946_30K_(187)	191	JoyP_PNC_0938_30K_(191)	--	647951	4/4/2026 12:47	97.48	Donald Chambers
340880	645980	1	272	No	Approved	Linked	--	190	JoyP_PNC_0953_30K_(190)	191	JoyP_PNC_0938_30K_(191)	--	647961	4/4/2026 13:18	89.98	Ventzilav Dimitrov
338445	639907	5	265	No	Approved	Linked	--	188	JoyP_PNC_0961_30K_(188)	191	JoyP_PNC_0938_30K_(191)	--	647969	4/4/2026 13:57	6.96	Debbie West
341817	642415	1	277	No	Declined	1st Recycle Failed	Insufficient funds	190	JoyP_PNC_0953_30K_(190)	191	JoyP_PNC_0938_30K_(191)	--	647982	4/4/2026 14:43	49.97	Will Davison
341788	642910	1	277	Yes	Declined	1st Recycle Failed	Insufficient funds	189	JoyP_PNC_0920_30K_(189)	191	JoyP_PNC_0938_30K_(191)	--	647936	4/4/2026 11:33	49.97	Steve Morton
341786	642416	1	282	No	Declined	1st Recycle Failed	Insufficient funds	191	JoyP_PNC_0938_30K_(191)	188	JoyP_PNC_0961_30K_(188)	--	647933	4/4/2026 11:22	49.97	Will Davison
341503	635970	3	274	No	Declined	2nd Recycle Failed	Insufficient funds	180	Closed-JoyP_EMS(harris)_0587_25K_(180)	188	JoyP_PNC_0961_30K_(188)	--	647944	4/4/2026 11:59	49.97	Roger D. Nation
341501	644363	1	267	No	Declined	2nd Recycle Failed	Insufficient funds	172	JoyP_Cliq(Avidia)_0687_20K_(172)	190	JoyP_PNC_0953_30K_(190)	--	647943	4/4/2026 11:52	59.97	Joel Cannon
341499	642414	1	272	No	Declined	2nd Recycle Failed	Insufficient funds	189	JoyP_PNC_0920_30K_(189)	188	JoyP_PNC_0961_30K_(188)	--	647938	4/4/2026 11:36	49.97	Will Davison
341487	644118	1	267	No	Declined	2nd Recycle Failed	Do Not Honor	190	JoyP_PNC_0953_30K_(190)	172	JoyP_Cliq(Avidia)_0687_20K_(172)	--	647921	4/4/2026 10:22	59.97	Phillip Winston
341264	646624	1	267	No	Declined	Linked	Issuer Declined	188	JoyP_PNC_0961_30K_(188)	190	JoyP_PNC_0953_30K_(190)	--	647918	4/4/2026 10:12	97.48	Albert Seeney
341263	646622	1	267	No	Declined	Linked	Issuer Declined	172	JoyP_Cliq(Avidia)_0687_20K_(172)	190	JoyP_PNC_0953_30K_(190)	--	647916	4/4/2026 10:00	97.48	Emilio Ramirez
341261	646619	1	267	No	Declined	Linked	Issuer Declined	172	JoyP_Cliq(Avidia)_0687_20K_(172)	187	JoyP_PNC_0946_30K_(187)	--	647919	4/4/2026 10:13	97.48	Bhupendra Panchal
341260	646609	1	287	No	Declined	Linked	Issuer Declined	191	JoyP_PNC_0938_30K_(191)	189	JoyP_PNC_0920_30K_(189)	--	647981	4/4/2026 14:41	6.96	Willie Moore
341256	646605	1	267	No	Declined	Linked	Issuer Declined	187	JoyP_PNC_0946_30K_(187)	189	JoyP_PNC_0920_30K_(189)	--	647963	4/4/2026 13:36	97.48	Willie Moore
341255	646603	1	267	No	Declined	Linked	Do Not Honor	190	JoyP_PNC_0953_30K_(190)	172	JoyP_Cliq(Avidia)_0687_20K_(172)	--	647931	4/4/2026 11:07	97.48	Codricas Campbell
341254	646602	1	287	No	Declined	Linked	Issuer Declined	189	JoyP_PNC_0920_30K_(189)	190	JoyP_PNC_0953_30K_(190)	--	647942	4/4/2026 11:45	6.96	Phillip Walters
341250	646597	1	267	No	Declined	Linked	Insufficient funds	172	JoyP_Cliq(Avidia)_0687_20K_(172)	190	JoyP_PNC_0953_30K_(190)	--	647986	4/4/2026 14:56	97.48	William Tuin
341248	646594	1	267	Yes	Declined	Linked	Issuer Declined	188	JoyP_PNC_0961_30K_(188)	189	JoyP_PNC_0920_30K_(189)	--	647962	4/4/2026 13:32	97.48	Roosevelt Hughes
341246	646588	1	267	Yes	Declined	Linked	Issuer Declined	190	JoyP_PNC_0953_30K_(190)	187	JoyP_PNC_0946_30K_(187)	--	647924	4/4/2026 10:27	97.48	Miles Henderson
341196	646490	1	267	No	Declined	Linked	Insufficient funds	172	JoyP_Cliq(Avidia)_0687_20K_(172)	190	JoyP_PNC_0953_30K_(190)	--	647986	4/4/2026 14:56	97.48	William Tuin
340778	645764	1	272	No	Declined	Linked	Issuer Declined	190	JoyP_PNC_0953_30K_(190)	189	JoyP_PNC_0920_30K_(189)	--	647948	4/4/2026 12:23	89.98	Jean Musypay
340615	645179	2	273	No	Declined	Linked	Issuer Declined	172	JoyP_Cliq(Avidia)_0687_20K_(172)	191	JoyP_PNC_0938_30K_(191)	--	647937	4/4/2026 11:35	89.98	Matthew Clark
340571	645114	2	273	No	Declined	Linked	Do Not Honor	191	JoyP_PNC_0938_30K_(191)	172	JoyP_Cliq(Avidia)_0687_20K_(172)	--	647967	4/4/2026 13:54	89.98	Donald Tanner
340544	645056	2	273	No	Declined	Linked	Issuer Declined	191	JoyP_PNC_0938_30K_(191)	189	JoyP_PNC_0920_30K_(189)	--	647979	4/4/2026 14:35	89.98	Rudi Ayala
340541	645049	2	273	Yes	Declined	Linked	Insufficient funds	191	JoyP_PNC_0938_30K_(191)	172	JoyP_Cliq(Avidia)_0687_20K_(172)	--	647964	4/4/2026 13:43	89.98	William Recor
340221	644306	2	268	No	Declined	Linked	Do Not Honor	190	JoyP_PNC_0953_30K_(190)	172	JoyP_Cliq(Avidia)_0687_20K_(172)	--	647952	4/4/2026 12:48	97.48	Jonathan Loyo
339740	643443	1	277	No	Declined	Linked	Issuer Declined	189	JoyP_PNC_0920_30K_(189)	191	JoyP_PNC_0938_30K_(191)	--	647980	4/4/2026 14:39	59.97	Ronald Cooks
339720	643425	1	282	No	Declined	Linked	Issuer Declined	190	JoyP_PNC_0953_30K_(190)	187	JoyP_PNC_0946_30K_(187)	--	647949	4/4/2026 12:28	49.97	Tony Cummings
339719	643424	1	277	No	Declined	Linked	Issuer Declined	189	JoyP_PNC_0920_30K_(189)	188	JoyP_PNC_0961_30K_(188)	--	647929	4/4/2026 10:46	59.97	Tony Cummings
338516	640133	4	256	Yes	Declined	Linked	Issuer Declined	189	JoyP_PNC_0920_30K_(189)	190	JoyP_PNC_0953_30K_(190)	--	647954	4/4/2026 12:56	59.97	Sandra Kowtko
338175	638992	4	256	Yes	Declined	Linked	Issuer Declined	172	JoyP_Cliq(Avidia)_0687_20K_(172)	187	JoyP_PNC_0946_30K_(187)	--	647974	4/4/2026 14:12	59.97	Debra Waterbury
341818	645411	1	272	No	Declined_AFR	1st Recycle Failed	Bad Bin or Host Disconnect	188	JoyP_PNC_0961_30K_(188)	200	Ridge_KURV(HARRIS)_4660_25K_(200)	--	647983	4/4/2026 14:45	79.97	Glenn Tate
341814	643631	2	268	No	Declined_AFR	1st Recycle Failed	Bad Bin or Host Disconnect	189	JoyP_PNC_0920_30K_(189)	198	Ridge_KURV(HARRIS)_5014_25K_(198)	--	647977	4/4/2026 14:30	79.97	John Byrd
341810	646291	1	267	No	Declined_AFR	1st Recycle Failed	Issuer Declined	191	JoyP_PNC_0938_30K_(191)	189	JoyP_PNC_0920_30K_(189)	--	647970	4/4/2026 13:58	79.97	Randy Clark
341809	645059	1	272	No	Declined_AFR	1st Recycle Failed	Bad Bin or Host Disconnect	187	JoyP_PNC_0946_30K_(187)	200	Ridge_KURV(HARRIS)_4660_25K_(200)	--	647966	4/4/2026 13:54	79.97	Earnest Parker sr
341805	646296	1	267	Yes	Declined_AFR	1st Recycle Failed	Activity limit exceeded	191	JoyP_PNC_0938_30K_(191)	172	JoyP_Cliq(Avidia)_0687_20K_(172)	--	647957	4/4/2026 13:11	79.97	John Taylor
341804	646179	1	267	Yes	Declined_AFR	1st Recycle Failed	Issuer Declined	190	JoyP_PNC_0953_30K_(190)	188	JoyP_PNC_0961_30K_(188)	--	647956	4/4/2026 13:09	79.97	Erik Taylor
341802	645400	1	272	No	Declined_AFR	1st Recycle Failed	Bad Bin or Host Disconnect	190	JoyP_PNC_0953_30K_(190)	196	Ridge_KURV(HARRIS)_4538_25K_(196)	--	647955	4/4/2026 13:03	79.97	Lynn Spears
341799	638242	4	256	No	Declined_AFR	1st Recycle Failed	Issuer Declined	182	Closed-JoyP_EMS(harris)_0595_25K_(182)	187	JoyP_PNC_0946_30K_(187)	--	647950	4/4/2026 12:47	59.97	Alice Griffin
341794	646157	1	267	No	Declined_AFR	1st Recycle Failed	Bad Bin or Host Disconnect	172	JoyP_Cliq(Avidia)_0687_20K_(172)	198	Ridge_KURV(HARRIS)_5014_25K_(198)	--	647947	4/4/2026 12:21	79.97	Kenneth Hudson Jr
341782	638432	4	256	Yes	Declined_AFR	1st Recycle Failed	Issuer Declined	190	JoyP_PNC_0953_30K_(190)	189	JoyP_PNC_0920_30K_(189)	--	647927	4/4/2026 10:42	59.97	Valorie Smith
341781	646287	1	267	No	Declined_AFR	1st Recycle Failed	Issuer Declined	187	JoyP_PNC_0946_30K_(187)	188	JoyP_PNC_0961_30K_(188)	--	647926	4/4/2026 10:41	79.97	Lisa Vaughn
341780	644561	2	273	Yes	Declined_AFR	1st Recycle Failed	Issuer Declined	172	JoyP_Cliq(Avidia)_0687_20K_(172)	190	JoyP_PNC_0953_30K_(190)	--	647922	4/4/2026 10:25	79.97	Antonio Jordan
341779	645384	1	272	No	Declined_AFR	1st Recycle Failed	Issuer Declined	190	JoyP_PNC_0953_30K_(190)	191	JoyP_PNC_0938_30K_(191)	--	647920	4/4/2026 10:13	79.97	Hector Rauda
341778	646301	1	267	No	Declined_AFR	1st Recycle Failed	Issuer Declined	172	JoyP_Cliq(Avidia)_0687_20K_(172)	190	JoyP_PNC_0953_30K_(190)	--	647917	4/4/2026 10:06	79.97	Christian Holden
341889	646490	1	267	No	Queue	1st Decline Recycling	--	172	JoyP_Cliq(Avidia)_0687_20K_(172)	190	JoyP_PNC_0953_30K_(190)	--	--	4/6/2026 14:56	79.97	William Tuin
341887	646609	1	287	No	Queue	1st Decline Recycling	--	191	JoyP_PNC_0938_30K_(191)	187	JoyP_PNC_0946_30K_(187)	--	--	4/6/2026 14:41	6.96	Willie Moore
341886	643443	1	277	No	Queue	1st Decline Recycling	--	189	JoyP_PNC_0920_30K_(189)	196	Ridge_KURV(HARRIS)_4538_25K_(196)	--	--	4/6/2026 14:39	49.97	Ronald Cooks
341885	645056	2	273	No	Queue	1st Decline Recycling	--	191	JoyP_PNC_0938_30K_(191)	198	Ridge_KURV(HARRIS)_5014_25K_(198)	--	--	4/6/2026 14:35	79.97	Rudi Ayala
341883	638992	4	256	Yes	Queue	1st Decline Recycling	--	172	JoyP_Cliq(Avidia)_0687_20K_(172)	191	JoyP_PNC_0938_30K_(191)	--	--	4/6/2026 14:12	59.97	Debra Waterbury
341882	646597	1	267	No	Queue	1st Decline Recycling	--	189	JoyP_PNC_0920_30K_(189)	200	Ridge_KURV(HARRIS)_4660_25K_(200)	--	--	4/6/2026 14:01	79.97	Phillip Walters
341881	645114	2	273	No	Queue	1st Decline Recycling	--	191	JoyP_PNC_0938_30K_(191)	187	JoyP_PNC_0946_30K_(187)	--	--	4/6/2026 13:54	79.97	Donald Tanner
341878	645049	2	273	Yes	Queue	1st Decline Recycling	--	191	JoyP_PNC_0938_30K_(191)	187	JoyP_PNC_0946_30K_(187)	--	--	4/6/2026 13:43	79.97	William Recor
341877	646605	1	267	No	Queue	1st Decline Recycling	--	187	JoyP_PNC_0946_30K_(187)	199	Ridge_KURV(HARRIS)_7432_25K_(199)	--	--	4/6/2026 13:36	79.97	Willie Moore
341876	646594	1	267	Yes	Queue	1st Decline Recycling	--	188	JoyP_PNC_0961_30K_(188)	198	Ridge_KURV(HARRIS)_5014_25K_(198)	--	--	4/6/2026 13:32	79.97	Roosevelt Hughes
341874	640133	4	256	Yes	Queue	1st Decline Recycling	--	189	JoyP_PNC_0920_30K_(189)	188	JoyP_PNC_0961_30K_(188)	--	--	4/6/2026 12:56	59.97	Sandra Kowtko
341873	644306	2	268	No	Queue	1st Decline Recycling	--	190	JoyP_PNC_0953_30K_(190)	198	Ridge_KURV(HARRIS)_5014_25K_(198)	--	--	4/6/2026 12:48	79.97	Jonathan Loyo
341872	643425	1	282	No	Queue	1st Decline Recycling	--	190	JoyP_PNC_0953_30K_(190)	197	Ridge_KURV(HARRIS)_4652_25K_(197)	--	--	4/6/2026 12:28	49.97	Tony Cummings
341871	645764	1	272	No	Queue	1st Decline Recycling	--	190	JoyP_PNC_0953_30K_(190)	188	JoyP_PNC_0961_30K_(188)	--	--	4/6/2026 12:23	79.97	Jean Musypay
341868	646602	1	287	No	Queue	1st Decline Recycling	--	189	JoyP_PNC_0920_30K_(189)	187	JoyP_PNC_0946_30K_(187)	--	--	4/6/2026 11:45	6.96	Phillip Walters
341866	645179	2	273	No	Queue	1st Decline Recycling	--	172	JoyP_Cliq(Avidia)_0687_20K_(172)	189	JoyP_PNC_0920_30K_(189)	--	--	4/6/2026 11:35	79.97	Matthew Clark
341862	646603	1	267	No	Queue	1st Decline Recycling	--	190	JoyP_PNC_0953_30K_(190)	198	Ridge_KURV(HARRIS)_5014_25K_(198)	--	--	4/6/2026 11:07	79.97	Codricas Campbell
341861	643424	1	277	No	Queue	1st Decline Recycling	--	189	JoyP_PNC_0920_30K_(189)	198	Ridge_KURV(HARRIS)_5014_25K_(198)	--	--	4/6/2026 10:46	49.97	Tony Cummings
341860	646588	1	267	Yes	Queue	1st Decline Recycling	--	190	JoyP_PNC_0953_30K_(190)	199	Ridge_KURV(HARRIS)_7432_25K_(199)	--	--	4/6/2026 10:27	79.97	Miles Henderson
341858	646619	1	267	No	Queue	1st Decline Recycling	--	172	JoyP_Cliq(Avidia)_0687_20K_(172)	187	JoyP_PNC_0946_30K_(187)	--	--	4/6/2026 10:13	79.97	Bhupendra Panchal
341857	646624	1	267	No	Queue	1st Decline Recycling	--	188	JoyP_PNC_0961_30K_(188)	191	JoyP_PNC_0938_30K_(191)	--	--	4/6/2026 10:12	79.97	Albert Seeney
341856	646622	1	267	No	Queue	1st Decline Recycling	--	172	JoyP_Cliq(Avidia)_0687_20K_(172)	190	JoyP_PNC_0953_30K_(190)	--	--	4/6/2026 10:00	79.97	Emilio Ramirez
341850	645517	1	272	No	Queue	1st Decline Recycling	--	188	JoyP_PNC_0961_30K_(188)	191	JoyP_PNC_0938_30K_(191)	--	--	4/5/2026 14:45	79.97	Randy England
341848	645675	1	272	No	Queue	1st Decline Recycling	--	189	JoyP_PNC_0920_30K_(189)	191	JoyP_PNC_0938_30K_(191)	--	--	4/5/2026 14:34	79.97	Larry Farmer
341847	645635	1	272	No	Queue	1st Decline Recycling	--	190	JoyP_PNC_0953_30K_(190)	199	Ridge_KURV(HARRIS)_7432_25K_(199)	--	--	4/5/2026 14:24	79.97	Hank Carr
341845	644773	2	273	No	Queue	1st Decline Recycling	--	172	JoyP_Cliq(Avidia)_0687_20K_(172)	191	JoyP_PNC_0938_30K_(191)	--	--	4/5/2026 14:18	79.97	John Byrd
341844	645650	1	272	No	Queue	1st Decline Recycling	--	190	JoyP_PNC_0953_30K_(190)	187	JoyP_PNC_0946_30K_(187)	--	--	4/5/2026 14:10	79.97	ASSEFA Seyoum
341838	644027	2	268	No	Queue	1st Decline Recycling	--	187	JoyP_PNC_0946_30K_(187)	190	JoyP_PNC_0953_30K_(190)	--	--	4/5/2026 13:18	79.97	Harold Allen
341836	646457	1	267	No	Queue	1st Decline Recycling	--	191	JoyP_PNC_0938_30K_(191)	198	Ridge_KURV(HARRIS)_5014_25K_(198)	--	--	4/5/2026 13:06	79.97	Mark Adkins
341833	645565	1	272	No	Queue	1st Decline Recycling	--	187	JoyP_PNC_0946_30K_(187)	189	JoyP_PNC_0920_30K_(189)	--	--	4/5/2026 13:00	79.97	Iren Lytle
341832	645740	1	272	No	Queue	1st Decline Recycling	--	187	JoyP_PNC_0946_30K_(187)	189	JoyP_PNC_0920_30K_(189)	--	--	4/5/2026 12:50	79.97	Gabino Trinidad
341830	646466	1	267	No	Queue	1st Decline Recycling	--	172	JoyP_Cliq(Avidia)_0687_20K_(172)	199	Ridge_KURV(HARRIS)_7432_25K_(199)	--	--	4/5/2026 12:43	79.97	Edward Hurd
341829	644039	2	268	No	Queue	1st Decline Recycling	--	172	JoyP_Cliq(Avidia)_0687_20K_(172)	198	Ridge_KURV(HARRIS)_5014_25K_(198)	--	--	4/5/2026 11:27	79.97	Tyrone Mason
341828	645667	1	272	Yes	Queue	1st Decline Recycling	--	191	JoyP_PNC_0938_30K_(191)	187	JoyP_PNC_0946_30K_(187)	--	--	4/5/2026 11:26	79.97	Timothy Collier
341827	643961	2	268	No	Queue	1st Decline Recycling	--	187	JoyP_PNC_0946_30K_(187)	196	Ridge_KURV(HARRIS)_4538_25K_(196)	--	--	4/5/2026 11:23	79.97	Matthew Clark
341826	646307	1	267	No	Queue	1st Decline Recycling	--	188	JoyP_PNC_0961_30K_(188)	187	JoyP_PNC_0946_30K_(187)	--	--	4/5/2026 11:09	79.97	Henry Miller
341825	646311	1	267	Yes	Queue	1st Decline Recycling	--	191	JoyP_PNC_0938_30K_(191)	190	JoyP_PNC_0953_30K_(190)	--	--	4/5/2026 10:59	79.97	Santos Nuncio
341824	645716	1	272	Yes	Queue	1st Decline Recycling	--	187	JoyP_PNC_0946_30K_(187)	196	Ridge_KURV(HARRIS)_4538_25K_(196)	--	--	4/5/2026 10:48	79.97	Charles Johnson
341823	646472	1	267	Yes	Queue	1st Decline Recycling	--	189	JoyP_PNC_0920_30K_(189)	188	JoyP_PNC_0961_30K_(188)	--	--	4/5/2026 10:46	79.97	Bryan Chapman
341545	647194	1	120	Yes	Queue	Linked	--	193	Ridge_SYN(PPS)_5575_50K_(193)	188	JoyP_PNC_0961_30K_(188)	--	--	4/6/2026 22:00	129.97	Doug Rosenberry
"""


def main():
    print("=" * 70)
    print("BinRoute AI — Pending Order Scoring")
    print("=" * 70)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # Parse orders
    orders = parse_orders()
    processed = [o for o in orders if o['status'] in ('Approved', 'Declined', 'Declined_AFR')]
    queued = [o for o in orders if o['status'] == 'Queue']

    print(f"\n  Total orders: {len(orders)}")
    print(f"  Already processed: {len(processed)} (Approved: {sum(1 for o in processed if o['status']=='Approved')}, "
          f"Declined: {sum(1 for o in processed if o['status'] != 'Approved')})")
    print(f"  In queue: {len(queued)}")

    # Load model + encoders
    print("\n[1] Loading model and fitting encoders...")
    sess, encoders, all_txf = load_model_and_encoders(conn)

    # Load gateway metadata
    gateways = load_gateways(conn)
    active_gw_ids = [gw_id for gw_id, gw in gateways.items()
                     if gw['processor_name'] and not gw['gateway_alias'].startswith('Closed')]

    print(f"  Active gateways: {len(active_gw_ids)}")

    # Match orders to customer BIN data
    print("\n[2] Matching orders to customer card data...")
    enrich_orders(orders, conn)

    matched = sum(1 for o in orders if o.get('cc_first_6'))
    print(f"  Matched {matched}/{len(orders)} orders to BIN data")

    # --- SECTION 1: Already processed orders ---
    print("\n" + "=" * 70)
    print("SECTION 1: ALREADY PROCESSED — AI vs Reality")
    print("=" * 70)

    for o in processed:
        if not o.get('cc_first_6'):
            continue
        score_order(o, gateways, active_gw_ids, sess, encoders, all_txf, show_reality=True)

    # --- SECTION 2: Queued orders ---
    print("\n" + "=" * 70)
    print("SECTION 2: QUEUED ORDERS — AI RECOMMENDATIONS")
    print("=" * 70)

    recommendations = []
    for o in queued:
        if not o.get('cc_first_6'):
            print(f"\n  [{o['sub_id']}] {o['customer']} — NO BIN DATA, skipping")
            continue
        rec = score_order(o, gateways, active_gw_ids, sess, encoders, all_txf, show_reality=False)
        if rec:
            recommendations.append(rec)

    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY — Queued Orders Where AI Disagrees With Current Routing")
    print("=" * 70)

    disagree = [r for r in recommendations if r.get('ai_disagrees')]
    print(f"\n  AI agrees with current routing: {len(recommendations) - len(disagree)}/{len(recommendations)}")
    print(f"  AI recommends different gateway: {len(disagree)}/{len(recommendations)}")

    if disagree:
        print(f"\n  {'Customer':<25} {'Current GW':>12} {'Curr %':>7} {'AI Best GW':>12} {'AI %':>7} {'Lift':>7}")
        print("  " + "-" * 73)
        for r in sorted(disagree, key=lambda x: x['lift_pp'], reverse=True):
            print(f"  {r['customer']:<25} {r['current_gw']:>12} {r['current_pct']:>6.1f}% "
                  f"{r['best_gw']:>12} {r['best_pct']:>6.1f}% {r['lift_pp']:>+6.1f}%")

    conn.close()
    print("\nDone!")


def parse_orders():
    """Parse the embedded order data."""
    orders = []
    for line in ORDERS_RAW.strip().split('\n'):
        parts = line.split('\t')
        if len(parts) < 17:
            continue
        orders.append({
            'sub_id': parts[0].strip(),
            'parent_order_id': int(parts[1].strip()),
            'billing_cycle': int(parts[2].strip()),
            'product_id': int(parts[3].strip()),
            'prepaid': parts[4].strip(),
            'status': parts[5].strip(),
            'tags': parts[6].strip(),
            'decline_reason': parts[7].strip() if parts[7].strip() != '--' else None,
            'current_gw_id': int(parts[8].strip()) if parts[8].strip() != '--' else None,
            'current_gw_name': parts[9].strip(),
            'assigned_gw_id': int(parts[10].strip()) if parts[10].strip() != '--' else None,
            'assigned_gw_name': parts[11].strip(),
            'forced_gw_id': int(parts[12].strip()) if parts[12].strip() != '--' else None,
            'new_order_id': int(parts[13].strip()) if parts[13].strip() != '--' else None,
            'est_date': parts[14].strip(),
            'price': float(parts[15].strip()),
            'customer': parts[16].strip(),
        })
    return orders


def load_model_and_encoders(conn):
    """Load ONNX model and fit encoders on all tx_features data."""
    all_txf = pd.read_sql_query("""
        SELECT * FROM tx_features WHERE feature_version >= 2
        ORDER BY acquisition_date ASC
    """, conn)

    encoders = {}
    for col in CATEGORICAL_FEATURES:
        le = LabelEncoder()
        le.fit(all_txf[col].fillna('UNKNOWN').astype(str))
        encoders[col] = le

    sess = ort.InferenceSession(MODEL_PATH)
    return sess, encoders, all_txf


def load_gateways(conn):
    """Load all gateways for Kytsan (client_id=1)."""
    rows = conn.execute("""
        SELECT gateway_id, gateway_alias, processor_name, bank_name, mcc_code
        FROM gateways WHERE client_id = 1
    """).fetchall()
    return {r['gateway_id']: dict(r) for r in rows}


def enrich_orders(orders, conn):
    """Match orders to customer card data via parent_order_id."""
    parent_ids = [o['parent_order_id'] for o in orders]
    placeholders = ','.join('?' * len(parent_ids))

    rows = conn.execute(f"""
        SELECT o.order_id, o.customer_id, o.cc_first_6, o.cc_type,
               o.derived_product_role, o.derived_cycle, o.derived_attempt,
               o.processing_gateway_id,
               b.issuer_bank, b.card_brand, b.card_type, b.is_prepaid
        FROM orders o
        LEFT JOIN bin_lookup b ON b.bin = o.cc_first_6
        WHERE o.client_id = 1 AND o.order_id IN ({placeholders})
    """, parent_ids).fetchall()

    lookup = {r['order_id']: dict(r) for r in rows}

    for o in orders:
        match = lookup.get(o['parent_order_id'])
        if match:
            o['customer_id'] = match['customer_id']
            o['cc_first_6'] = match['cc_first_6']
            o['issuer_bank'] = match['issuer_bank']
            o['card_brand'] = match['card_brand']
            o['card_type_bin'] = match['card_type']
            o['is_prepaid_bin'] = match['is_prepaid'] or 0
            o['derived_product_role'] = match['derived_product_role']
            o['derived_cycle'] = match['derived_cycle']
            o['derived_attempt'] = match['derived_attempt']

    # Also get initial processor for each customer
    cust_ids = list(set(o.get('customer_id') for o in orders if o.get('customer_id')))
    if cust_ids:
        placeholders2 = ','.join('?' * len(cust_ids))
        init_rows = conn.execute(f"""
            SELECT o.customer_id, g.processor_name
            FROM orders o
            JOIN gateways g ON g.client_id = o.client_id AND g.gateway_id = o.processing_gateway_id
            WHERE o.client_id = 1
              AND o.derived_product_role = 'main_initial'
              AND o.order_status IN (2, 6, 8)
              AND o.customer_id IN ({placeholders2})
            ORDER BY o.acquisition_date ASC
        """, cust_ids).fetchall()

        init_proc = {}
        for r in init_rows:
            if r['customer_id'] not in init_proc:
                init_proc[r['customer_id']] = r['processor_name']

        for o in orders:
            cid = o.get('customer_id')
            if cid and cid in init_proc:
                o['initial_processor'] = init_proc[cid]

    # Get subscription features from tx_features for each customer's parent order
    for o in orders:
        parent = o['parent_order_id']
        sub_row = conn.execute("""
            SELECT consecutive_approvals, days_since_last_charge, days_since_initial,
                   lifetime_charges, lifetime_revenue, initial_amount, amount_ratio,
                   prior_declines_in_cycle
            FROM tx_features
            WHERE client_id = 1 AND sticky_order_id = ?
        """, [parent]).fetchone()
        if sub_row:
            o['consecutive_approvals'] = sub_row['consecutive_approvals'] or 0
            o['days_since_last_charge'] = sub_row['days_since_last_charge'] or 0
            o['days_since_initial'] = sub_row['days_since_initial'] or 0
            o['lifetime_charges'] = sub_row['lifetime_charges'] or 0
            o['lifetime_revenue'] = sub_row['lifetime_revenue'] or 0
            o['initial_amount'] = sub_row['initial_amount'] or 0
            o['amount_ratio'] = sub_row['amount_ratio'] or 0
            o['prior_declines_in_cycle'] = sub_row['prior_declines_in_cycle'] or 0


def score_order(order, gateways, active_gw_ids, sess, encoders, all_txf, show_reality=True):
    """Score an order across all available gateways."""
    from datetime import datetime

    # Parse estimated date for hour/day
    try:
        dt = datetime.strptime(order['est_date'], '%m/%d/%Y %H:%M')
        hour = dt.hour
        dow = dt.weekday()  # Mon=0 → convert to Sun=0
        dow = (dow + 1) % 7
    except:
        hour = 12
        dow = 0

    # Determine tx_class
    attempt = order.get('derived_attempt', 1) or 1
    tag = order['tags']
    if '1st' in tag:
        attempt_num = max(attempt, 2)
    elif '2nd' in tag:
        attempt_num = max(attempt, 3)
    elif '3rd' in tag:
        attempt_num = max(attempt, 4)
    else:
        attempt_num = attempt

    tx_class = 'salvage'  # most of these are retry/recycle
    if attempt_num == 1:
        role = order.get('derived_product_role', '')
        if role == 'main_initial':
            tx_class = 'initial'
        elif role == 'upsell_initial':
            tx_class = 'upsell'
        elif role in ('main_rebill', 'upsell_rebill'):
            tx_class = 'rebill'

    cycle = order.get('derived_cycle', 0) or 0
    if cycle == 0:
        cycle_depth = 'C0'
    elif cycle == 1:
        cycle_depth = 'C1'
    elif cycle == 2:
        cycle_depth = 'C2'
    else:
        cycle_depth = 'C3+'

    # Normalize issuer bank
    issuer = order.get('issuer_bank', 'Unknown') or 'Unknown'
    u_issuer = issuer.upper()
    if 'BANK OF AMERICA' in u_issuer:
        issuer = 'BANK OF AMERICA, NATIONAL ASSOCIATION'
    elif 'CITIBANK' in u_issuer or 'CITI BANK' in u_issuer:
        issuer = 'CITIBANK N.A.'
    elif 'JPMORGAN' in u_issuer or 'JP MORGAN' in u_issuer:
        issuer = 'JPMORGAN CHASE BANK N.A.'

    prev_decline = order.get('decline_reason')
    init_proc = order.get('initial_processor')
    if init_proc:
        init_proc = init_proc.strip().upper()

    # Gateways not available for rebills/salvage
    REBILL_EXCLUDED_GW = {192}  # PAYFAC — can't be used on rebills

    # Score each gateway
    results = []
    for gw_id in active_gw_ids:
        gw = gateways.get(gw_id)
        if not gw or not gw['processor_name']:
            continue
        # Skip PAYFAC for non-initial tx classes
        if gw_id in REBILL_EXCLUDED_GW and tx_class in ('rebill', 'salvage', 'cascade'):
            continue

        proc = gw['processor_name'].strip().upper()
        bank = gw.get('bank_name') or 'UNKNOWN'
        mcc = gw.get('mcc_code') or 'UNKNOWN'

        features = {
            'processor_name': proc,
            'acquiring_bank': bank,
            'mcc_code': mcc,
            'issuer_bank': issuer,
            'card_brand': order.get('card_brand', 'UNKNOWN') or 'UNKNOWN',
            'card_type': order.get('card_type_bin', 'UNKNOWN') or 'UNKNOWN',
            'tx_class': tx_class,
            'cycle_depth': cycle_depth,
            'prev_decline_reason': prev_decline or 'UNKNOWN',
            'initial_processor': init_proc or 'UNKNOWN',
            'is_prepaid': order.get('is_prepaid_bin', 0),
            'amount': order['price'],
            'attempt_number': attempt_num,
            'hour_of_day': hour,
            'day_of_week': dow,
            'mid_velocity_daily': 200,  # approximate current load
            'mid_velocity_weekly': 1400,
            'customer_history_on_proc': 1,
            'bin_velocity_weekly': 100,
            # Subscription features
            'consecutive_approvals': order.get('consecutive_approvals', 0),
            'days_since_last_charge': order.get('days_since_last_charge', 0),
            'days_since_initial': order.get('days_since_initial', 0),
            'lifetime_charges': order.get('lifetime_charges', 0),
            'lifetime_revenue': order.get('lifetime_revenue', 0),
            'initial_amount': order.get('initial_amount', 0),
            'amount_ratio': order.get('amount_ratio', 0),
            'prior_declines_in_cycle': order.get('prior_declines_in_cycle', 0),
        }

        # Encode
        encoded = []
        for col in CATEGORICAL_FEATURES:
            val = features[col]
            le = encoders[col]
            if val in le.classes_:
                encoded.append(le.transform([val])[0])
            else:
                # Unknown category → use 'UNKNOWN' if available, else 0
                if 'UNKNOWN' in le.classes_:
                    encoded.append(le.transform(['UNKNOWN'])[0])
                else:
                    encoded.append(0)

        for col in NUMERICAL_FEATURES:
            encoded.append(features[col] or 0)

        X = np.array([encoded], dtype=np.float32)

        raw = sess.run(None, {sess.get_inputs()[0].name: X})
        probs = raw[1]
        if isinstance(probs, list) and isinstance(probs[0], dict):
            prob = probs[0].get(1, probs[0].get('1', 0))
        elif hasattr(probs, 'shape') and len(probs.shape) == 2:
            prob = probs[0][1]
        else:
            prob = float(probs[0])

        results.append({
            'gw_id': gw_id,
            'gw_name': gw.get('gateway_alias', str(gw_id)),
            'processor': proc,
            'prob': prob,
        })

    results.sort(key=lambda x: x['prob'], reverse=True)

    # Current gateway score
    current_gw = order.get('current_gw_id')
    current_result = next((r for r in results if r['gw_id'] == current_gw), None)
    assigned_gw = order.get('assigned_gw_id')
    assigned_result = next((r for r in results if r['gw_id'] == assigned_gw), None)
    best = results[0] if results else None

    # Print
    status_icon = {'Approved': 'APPROVED', 'Declined': 'DECLINED', 'Declined_AFR': 'DECLINED(AFR)', 'Queue': 'QUEUED'}
    print(f"\n  [{order['sub_id']}] {order['customer']} — ${order['price']:.2f} — "
          f"{order.get('card_brand', '?')}/{order.get('card_type_bin', '?')} — "
          f"{issuer[:30]} — {tx_class} attempt {attempt_num}")

    if show_reality:
        actual = status_icon.get(order['status'], order['status'])
        print(f"    ACTUAL: {actual} on {order['current_gw_name']}")

    if current_result:
        print(f"    Current  [{current_gw}]: {current_result['processor']:<15} -> {current_result['prob']*100:5.1f}% predicted")
    if assigned_result and assigned_gw != current_gw:
        print(f"    Assigned [{assigned_gw}]: {assigned_result['processor']:<15} -> {assigned_result['prob']*100:5.1f}% predicted")
    if best and best['gw_id'] != current_gw:
        print(f"    AI BEST  [{best['gw_id']}]: {best['processor']:<15} -> {best['prob']*100:5.1f}% predicted")

    # Top 3
    print(f"    Top 3: ", end='')
    for i, r in enumerate(results[:3]):
        marker = ' *' if r['gw_id'] == current_gw else ''
        print(f"[{r['gw_id']}] {r['prob']*100:.1f}%{marker}  ", end='')
    print()

    # Return recommendation for queue orders
    if not show_reality and best and current_result:
        ai_disagrees = best['gw_id'] != current_gw and (best['prob'] - current_result['prob']) > 0.02
        return {
            'sub_id': order['sub_id'],
            'customer': order['customer'],
            'current_gw': str(current_gw),
            'current_pct': current_result['prob'] * 100,
            'best_gw': str(best['gw_id']),
            'best_pct': best['prob'] * 100,
            'lift_pp': (best['prob'] - current_result['prob']) * 100,
            'ai_disagrees': ai_disagrees,
        }
    return None


if __name__ == '__main__':
    main()
