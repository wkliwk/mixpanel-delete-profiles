# mixpanel-delete-profiles
Batch delete Mixpanel profiles. Read the `distinctId` from csv and construct delete mixpanel profile query then post to mixpanel engage api.

1. Prepare a `mixpanel-delete-profiles.csv` (require header `distinct_id`). You can query the distinctId by using Mixpanel JQL and then export as csv file:
```
distinct_id
AD1C353B-522F-4636-912A-1137A37C2949
13bc502a-de76-47a1-99b6-2a33854d22f5
53B06D76-036E-453D-9AAE-CCF381B3F583
8DDBE035-41A6-4F9C-A9B0-252BCBC9982F
F390480B-69E2-43EB-A441-EF1B195EBACA
```

2. `cd` to project

3. Install dependency : 
```
pip install pandas
pip install requests
```
4. Get your Mixpanel **Token** of your project from Mixpanel console (Project settings). 

5. Open `deleteMixpanelProfile.py` and replace the `TOKEN` variable value with your Token:

```
TOKEN = "your_mixpanel_token"
```

6. Run the python script:
```
python deleteMixpanelProfile.py
```

Mixpanel reference: https://mixpanel.com/help/reference/http#people-analytics-updates
