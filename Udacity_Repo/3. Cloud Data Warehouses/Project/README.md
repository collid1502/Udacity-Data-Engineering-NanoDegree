{
 "cells": [
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "## Sparkify Data Warehouse - ETL overview\n",
    "\n",
    "##### <font color='Yellow'>Introduction<font/>\n",
    "For the music streaming start-up, Sparkify, being able to understand the tastes and play habits of their user base is vital. With this in mind, the ETL project designed and discussed here looks to create a star schema series of analytics tables, on a database created within a Redshift cluster on Amazon Web Services.\n",
    "\n",
    "The benefit of cloud computing for this database, is that as the user base for Sparkify grows, and more data is captured over time, cloud resources can scale quickly to handle the future data needs of the company.\n",
    "\n",
    "The company needs to be able to understand & answer different questions relating to their user base. This can include what songs are most popular, what type of users are listening, are users interested in certain new content as its released, what pre-existing songs continue to prove popular over time and so on ..."
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "##### <font color='Yellow'>Schema Design<font/>\n",
    "    \n",
    "The schema design for the Sparkify database was a star schema approach, using one FACT and four DIMENSION tables. <br>\n",
    "The one fact table was a \"songplays\" table, essentially, a record of every songplay by each user across the Sparkify app. <br>\n",
    "The four dimension tables were to act as reference support for analytical queries against each songplay event. This allowed for a reduction in data redundancy, by storing data in separate reference tables that could be captured through simple joins via dimension keys."
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "The four reference tables were:\n",
    "- User : a table containing master information on each Sparkify user, with one distinct row per user\n",
    "- Songs : a table containing details of the songs played through the Sparkify app by users. This included information about the song such as title, length, release year etc.\n",
    "- Artists : a table containing details of the Artists behind each song played through the Sparkify App. Details provided included Name and location.\n",
    "- Time : a table containing useful time metrics such as day, month, year, week, weekday against each timestamp that exists within the Sparkify App for a songplay."
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "##### <font color='Yellow'>ETL Pipeline<font/>"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "The ETL pipeline operates by peforming the following actions:\n",
    "\n",
    "1. Launching a connection to a pre-determined database on a redshift cluster. This database, cluster & IAM role are all specified within the dwh.cfg config file.\n",
    "</p>\n",
    "2. Any pre-existing tables that may exist, either staging or analytic, are dropped\n",
    "</P>\n",
    "3. New tables are now created. Two staging tables, one FACT table and four DIMENSION tables. All are empty.\n",
    "</P>\n",
    "4. We then use a COPY command to load data relating to songs and songplay events from JSON files into the two staging tables: Events & Songs.\n",
    "</P>\n",
    "5. From these two staging tables, we can now populate the one FACT and four DIMENSION tables through a series of INSERT INTO statements which collect data from each staging table or a comnination fo the two. This approach is useful, as it allows us to bulk load our data first, typically in text format through the use of VARCHAR. Then, identify the specific data types that can exist for each FACT and DIMENSION table, from the staging tables, and transform data into the relevant data type as it is inserted."
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "##### <font color='Yellow'>Running the ETL Pipeline<font/>"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "The ETL pipeline can be ran through the notebook <font color='Green'>Run Project.ipynb<font/>\n",
    "    \n",
    "This notebook uses IaC (Infrastructure as Code) to launch the create and launch the Sparkify database on a redshift cluster. It also creates the relevant IAM role for access. <br>\n",
    "The code will then use magic commands to first, run the create_tables.py file, before then running the etl.py file."
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "These two python files call on a series of SQL statements written within the sql_queries.py file, and process each statment in the correct order, through a connection to the Sparkify database on the redshift cluster created.\n",
    "\n",
    "Finally, to end this project and clean up all resources, the Run Project notebook also contains the required code to wipe all tables, delete the cluster and remove the IAM role."
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "##### <font color='Yellow'>Some example analysis on tables from the Sparkify Database<font/>"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "*Where the value of user_id is 8, identify what songs have been listened to, who the artists are, and what weekday the songplay was on*"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 14,
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      " * postgresql://sparkifyadmin:***@sparkify1.cms6ffzqbc3y.us-west-2.redshift.amazonaws.com:5439/sparkify\n",
      "3 rows affected.\n"
     ]
    },
    {
     "data": {
      "text/html": [
       "<table>\n",
       "    <tr>\n",
       "        <th>user_id</th>\n",
       "        <th>first_name</th>\n",
       "        <th>last_name</th>\n",
       "        <th>level</th>\n",
       "        <th>songplay_id</th>\n",
       "        <th>song_id</th>\n",
       "        <th>artist_id</th>\n",
       "        <th>start_time</th>\n",
       "        <th>artist_name</th>\n",
       "        <th>weekday</th>\n",
       "    </tr>\n",
       "    <tr>\n",
       "        <td>8</td>\n",
       "        <td>Kaylee</td>\n",
       "        <td>Summers</td>\n",
       "        <td>free</td>\n",
       "        <td>647</td>\n",
       "        <td>SOEIQUY12AF72A086A</td>\n",
       "        <td>ARHUC691187B9AD27F</td>\n",
       "        <td>2018-11-01 21:11:13</td>\n",
       "        <td>The Mars Volta</td>\n",
       "        <td>4</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "        <td>8</td>\n",
       "        <td>Kaylee</td>\n",
       "        <td>Summers</td>\n",
       "        <td>free</td>\n",
       "        <td>5421</td>\n",
       "        <td>SOWEUOO12A6D4F6D0C</td>\n",
       "        <td>ARQUMH41187B9AF699</td>\n",
       "        <td>2018-11-27 04:25:00</td>\n",
       "        <td>Linkin Park</td>\n",
       "        <td>2</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "        <td>8</td>\n",
       "        <td>Kaylee</td>\n",
       "        <td>Summers</td>\n",
       "        <td>free</td>\n",
       "        <td>4271</td>\n",
       "        <td>SOWTZNU12AB017EADB</td>\n",
       "        <td>AR6NYHH1187B9BA128</td>\n",
       "        <td>2018-11-07 01:42:43</td>\n",
       "        <td>Yeah Yeah Yeahs</td>\n",
       "        <td>3</td>\n",
       "    </tr>\n",
       "</table>"
      ],
      "text/plain": [
       "[('8', 'Kaylee', 'Summers', 'free', 647, 'SOEIQUY12AF72A086A', 'ARHUC691187B9AD27F', datetime.datetime(2018, 11, 1, 21, 11, 13), 'The Mars Volta', '4'),\n",
       " ('8', 'Kaylee', 'Summers', 'free', 5421, 'SOWEUOO12A6D4F6D0C', 'ARQUMH41187B9AF699', datetime.datetime(2018, 11, 27, 4, 25), 'Linkin Park', '2'),\n",
       " ('8', 'Kaylee', 'Summers', 'free', 4271, 'SOWTZNU12AB017EADB', 'AR6NYHH1187B9BA128', datetime.datetime(2018, 11, 7, 1, 42, 43), 'Yeah Yeah Yeahs', '3')]"
      ]
     },
     "execution_count": 14,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "%%sql\n",
    "SELECT \n",
    "a.user_id, a.first_name, a.last_name, a.level, \n",
    "b.songplay_id, b.song_id, b.artist_id, b.start_time,\n",
    "c.name as Artist_Name,\n",
    "d.weekday \n",
    "\n",
    "FROM users as a \n",
    "LEFT JOIN songplays as b ON a.user_id = b.user_id \n",
    "LEFT JOIN artists as c ON b.artist_id = c.artist_id \n",
    "LEFT JOIN time as d ON b.start_time = d.start_time\n",
    "WHERE a.user_id = '8' \n",
    "AND b.song_id IS NOT NULL "
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "\n",
    "----------------\n"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "*Identify how many times the artist \"Linkin Park\" was played during Q4 2018*"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 15,
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      " * postgresql://sparkifyadmin:***@sparkify1.cms6ffzqbc3y.us-west-2.redshift.amazonaws.com:5439/sparkify\n",
      "1 rows affected.\n"
     ]
    },
    {
     "data": {
      "text/html": [
       "<table>\n",
       "    <tr>\n",
       "        <th>artist</th>\n",
       "        <th>number_of_plays_q42018</th>\n",
       "    </tr>\n",
       "    <tr>\n",
       "        <td>Linkin Park</td>\n",
       "        <td>4</td>\n",
       "    </tr>\n",
       "</table>"
      ],
      "text/plain": [
       "[('Linkin Park', 4)]"
      ]
     },
     "execution_count": 15,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "%%sql\n",
    "SELECT \n",
    "t1.name as Artist, \n",
    "count(t1.songplay_id) as number_of_plays_Q42018\n",
    "\n",
    "FROM \n",
    "(\n",
    "SELECT\n",
    "    a.artist_id, a.name, b.songplay_id, b.start_time, c.year \n",
    "FROM\n",
    "    artists as a \n",
    "LEFT JOIN \n",
    "    songplays as b \n",
    "ON a.artist_id = b.artist_id \n",
    "\n",
    "INNER JOIN\n",
    "    time as c \n",
    "ON b.start_time = c.start_time \n",
    "\n",
    "WHERE a.name = 'Linkin Park' AND c.year = '2018' AND c.month in ('10','11','12') \n",
    ") as t1 \n",
    "GROUP BY t1.name "
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    " "
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "End"
   ]
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.7.6"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 4
}
