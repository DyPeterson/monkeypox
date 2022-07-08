from pyspark.sql import SparkSession
import pyspark.sql.functions as sf
import pyspark.sql.types as st
from datetime import date
from pyspark.sql.functions import col, mean
from pyspark.sql.types import StringType


# Shows if a country has had at least one case with the most recent recorded date.
most_recent_df = daily_df.select('*').where(daily_df[-1] >= 1)

# Shows the change over 1 week from the most recent data that have had atleast cases last week.
one_week_change_df = daily_df.select('Country', daily_df[date_minus_weeks(2)],daily_df[date_minus_weeks(1)]).where(daily_df[date_minus_weeks(1)] > 10)

# Shows the weekly changes in cases starting from 7 weeks ago to last week.
weekly_df = daily_df.select(
        'Country',
        daily_df[date_minus_weeks(7)],
        daily_df[date_minus_weeks(6)],
        daily_df[date_minus_weeks(5)],
        daily_df[date_minus_weeks(4)],
        daily_df[date_minus_weeks(3)],
        daily_df[date_minus_weeks(2)],
        daily_df[date_minus_weeks(1)])

agg1_df = daily_df.drop('Country')
agg1_df = daily_df.withColumn('total', reduce(column_add, (agg1_df[col] for col in agg1_df.columns)))
agg1_df = agg1_df.groupBy().sum()

africa = ["Algeria", "Angola", "Benin", "Botswana", "Burkina Faso", "Burundi", "Cameroon", "Cape Verde", "Central African Republic","Chad", "Comoros", "Cote d'Ivoire", "Democratic Republic of the Congo", "Djibouti", "Egypt", "Equatorial Guinea", "Eritrea","Ethiopia", "Gabon", "Gambia", "Ghana", "Guinea", "Guinea-Bissau", "Kenya", "Lesotho", "Liberia", "Libya", "Madagascar", "Malawi","Mali", "Mauritania", "Mauritius", "Morocco", "Mozambique", "Namibia", "Niger", "Nigeria", "Republic of the Congo", "Reunion","Rwanda", "Saint Helena", "Sao Tome and Principe", "Senegal", "Seychelles", "Sierra Leone", "Somalia", "South Africa", "South Sudan", "Sudan", "Swaziland", "Tanzania", "Togo", "Tunisia", "Uganda", "Western Sahara", "Zambia", "Zimbabwe"]

asia = ["Afghanistan", "Armenia", "Azerbaijan", "Bahrain", "Bangladesh", "Bhutan", "Brunei", "Burma", "Cambodia", "China", "Cyprus","East Timor", "Georgia", "Hong Kong", "India", "Indonesia", "Iran", "Iraq", "Israel", "Japan", "Jordan", "Kazakhstan", "Kuwait","Kyrgyzstan", "Laos", "Lebanon", "Macau", "Malaysia", "Maldives", "Mongolia", "Nepal", "North Korea", "Oman", "Pakistan", "Philippines","Qatar", "Saudi Arabia", "Singapore", "South Korea", "Sri Lanka", "Syria", "Taiwan", "Tajikistan", "Thailand", "Turkey", "Turkmenistan","United Arab Emirates", "Uzbekistan", "Vietnam", "Yemen"]

caribbean = ["Anguilla", "Antigua and Barbuda", "Aruba", "The Bahamas", "Barbados", "Bermuda", "British Virgin Islands", "Cayman Islands","Cuba", "Dominica", "Dominican Republic", "Grenada", "Guadeloupe", "Haiti", "Jamaica", "Martinique", "Montserrat", "Netherlands Antilles","Puerto Rico", "Saint Kitts and Nevis", "Saint Lucia", "Saint Vincent and the Grenadines", "Trinidad and Tobago", "Turks and Caicos Islands", "U.S. Virgin Islands"]

central_america = ["Belize", "Costa Rica", "El Salvador", "Guatemala", "Honduras", "Nicaragua", "Panama"]

europe = ["Albania", "Andorra", "Austria", "Belarus", "Belgium", "Bosnia and Herzegovina", "Bulgaria", "Croatia", "Czech Republic", "Denmark", "England", "Estonia", "Finland", "France", "Germany", "Gibraltar", "Greece", "Holy See", "Hungary", "Iceland", "Ireland", "Italy", "Kosovo", "Latvia","Liechtenstein", "Lithuania", "Luxembourg", "Macedonia", "Malta", "Moldova", "Monaco", "Montenegro", "Netherlands", "Norway", "Poland", "Portugal", "Romania", "Russia", "San Marino", "Slovak Republic", "Slovenia", "Spain", "Serbia", "Serbia and Montenegro", "Sweden", "Switzerland", "Ukraine", "United Kingdom", "Northern Ireland", "Wales"]

north_america = ["Canada", "Greenland", "Mexico", "Saint Pierre and Miquelon", "United States"]

oceania = ["American Samoa", "Australia", "Christmas Island", "Cocos (Keeling) Islands", "Cook Islands", "Federated States of Micronesia", "Fiji", "French Polynesia", "Guam", "Kiribati", "Marshall Islands", "Nauru", "New Caledonia", "New Zealand", "Niue", "Northern Mariana Islands", "Palau", "Papua New Guinea", "Pitcairn Islands","Samoa", "Solomon Islands", "Tokelau", "Tonga", "Tuvalu", "Vanuatu", "Wallis and Futuna Islands"]

south_america = ["Argentina", "Bolivia", "Brazil", "Chile", "Colombia", "Ecuador", "Falkland Islands", "French Guiana", "Guyana", "Paraguay", "Peru", "Suriname", "Uruguay","Venezuela"]

# Africa df
asia_df = daily_df.filter(daily_df['Country'].isin(asia))

asia_df = asia_df.agg(*[sf.sum(asia_df[c_name]) for c_name in asia_df.columns])

asia_df = asia_df.withColumn('sum(Country)', col('sum(Country)').cast(StringType()))

asia_df = asia_df.na.fill(value='asia_totals')

# Caribbean
caribbean_df = daily_df.where(daily_df['Country'].isin(caribbean))

caribbean_df = caribbean_df.agg(*[sf.sum(caribbean_df[c_name])for c_name in caribbean_df.columns])

caribbean_df = caribbean_df.withColumn('sum(Country)', col('sum(Country)').cast(StringType()))

caribbean_df = caribbean_df.na.fill(value='caribbean_totals')

# Central America
central_america_df = daily_df.where(daily_df['Country'].isin(central_america))

central_america_df = central_america_df.agg(*[sf.sum(central_america_df[c_name])for c_name in central_america_df.columns])

central_america_df = central_america_df.withColumn('sum(Country)', col('sum(Country)').cast(StringType()))

central_america_df = central_america_df.na.fill(value='central_america_totals')

# Europe
europe_df = daily_df.where(daily_df['Country'].isin(europe))

europe_df = europe_df.agg(*[sf.sum(europe_df[c_name])for c_name in europe_df.columns])

europe_df = europe_df.withColumn('sum(Country)', col('sum(Country)').cast(StringType()))

europe_df = europe_df.na.fill(value='europe_totals')

# North America
north_america_df = daily_df.where(daily_df['Country'].isin(north_america))

north_america_df = north_america_df.agg(*[sf.sum(north_america_df[c_name])for c_name in north_america_df.columns])

north_america_df = north_america_df.withColumn('sum(Country)', col('sum(Country)').cast(StringType()))

north_america_df = north_america_df.na.fill(value='north_america_totals')

# Oceania
oceania_df = daily_df.where(daily_df['Country'].isin(oceania))

oceania_df = oceania_df.agg(*[sf.sum(oceania_df[c_name])for c_name in oceania_df.columns])

oceania_df = oceania_df.withColumn('sum(Country)', col('sum(Country)').cast(StringType()))

oceania_df = oceania_df.na.fill(value='oceania_totals')

# South America
south_america_df = daily_df.where(daily_df['Country'].isin(south_america))

south_america_df = south_america_df.agg(*[sf.sum(south_america_df[c_name])for c_name in south_america_df.columns])

south_america_df = south_america_df.withColumn('sum(Country)', col('sum(Country)').cast(StringType()))

south_america_df = south_america_df.na.fill(value='south_america_totals')

# All region dataframe
region_df_list = ['south_america_df', 'oceania_df', 'north_america_df', 'asia_df', 'africa_df', 'caribbean_df', 'europe_df']

region_df = south_america_df.union(oceania_df)

region_df = region_df.union(north_america_df)

region_df = region_df.union(asia_df)

region_df = region_df.union(africa_df)

region_df = region_df.union(caribbean_df)

region_df = region_df.union(europe_df)

region_df = region_df.withColumnRenamed('sum(Country)', 'region')

# Daily stats followed by the total of each recorded date
dropped_df = region_df.drop('region')
agg2_df = region_df.withColumn('total', reduce(column_add, (dropped_df[col] for col in dropped_df.columns)))

#Total for each recorded date and total of totals for the world
world_total_df = agg2_df.groupBy().sum()

from pyspark.sql.functions import mean

#Averages of worldwide cases

agg3_df = agg2_df.groupBy().mean()
agg3_pd_df = agg3_df.toPandas()
agg3_pd_df = agg3_pd_df.round()