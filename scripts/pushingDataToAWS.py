import configparser
import json
import os
import pprint
import psycopg2

config = configparser.ConfigParser()
config.read('instances/config.py')


def readFromJSONFile(fileName):

    with open(fileName, 'r') as jsonData:
        data = json.load(jsonData)

        return data


def seeDataInDB(cursor):
    cursor.execute("""SELECT * from songs""")
    rows = cursor.fetchall()

    print("\nShow me the databases:\n")

    pprint.pprint(rows)


if __name__ == '__main__':
    try:
        conn_string = "host=" + config['DEFAULT']['DSN_HOSTNAME'] + " port=" + config['DEFAULT']['DSN_PORT'] + " dbname=" + config['DEFAULT']['DSN_DATABASE'] + " user=" + config['DEFAULT']['DSN_UID'] + " password=" + config['DEFAULT']['DSN_PWD']
        print("Connecting to database\n  ->%s" % (conn_string))

        conn = psycopg2.connect(conn_string)
        print("Connected!\n")
    except Exception as e:
        print("Unable to connect to the database.")
        print(e)

    cursor = conn.cursor()

    songdataJSON = 'songHelixData_v0314.json'
    dataDirectory = './data'
    filePath = os.path.join(dataDirectory, songdataJSON)

    songdata = readFromJSONFile(filePath)

    # cursor.execute("CREATE TABLE songs("
    #                "ID INT PRIMARY KEY NOT NULL,"
    #                "song_title CHAR(100) NOT NULL,"
    #                "composer CHAR(100),"
    #                "poet_or_lyricist CHAR(100),"
    #                "features TEXT,"
    #                "keywords TEXT,"
    #                "larger_Work_original_publication TEXT,"
    #                "poets_associated_movements_or_isms_or_groups TEXT,"
    #                "musical_form TEXT,"
    #                "first_line TEXT,"
    #                "year_of_composition CHAR(50),"
    #                "catalog_designation TEXT,"
    #                "complete_edition_reference TEXT,"
    #                "composer_place_of_birth CHAR(100),"
    #                "original_language CHAR(100),"
    #                "voice_part_suggested_by_the_composer TEXT,"
    #                "Berton_Coffin_suggested_voice_type TEXT,"
    #                "Berton_Coffin_suggested_song_type TEXT,"
    #                "piano_and_voice_only CHAR(100),"
    #                "orchestra_and_voice CHAR(100),"
    #                "other_instrumentation_and_voice TEXT,"
    #                "more_than_one_voice TEXT,"
    #                "original_key TEXT,"
    #                "range_in_original_key TEXT,"
    #                "dedicated_to CHAR(100),"
    #                "premiered_by CHAR(100),"
    #                "commissioned_by CHAR(100),"
    #                "average_duration CHAR(50),"
    #                "degrees_location_relationship_occupation TEXT,"
    #                "recommended_printed_source TEXT,"
    #                "recommended_translation_source TEXT,"
    #                "score_source TEXT,"
    #                "audio_source TEXT,"
    #                "other_musical_allusions_accompaniment_figures TEXT,"
    #                "contributor CHAR(100),"
    #                "orderID CHAR(50)"
    #                ")")

    cursor.execute("CREATE TABLE songs("
                   "id SERIAL PRIMARY KEY,"
                   "song_title CHAR(100) NOT NULL,"
                   "composer_id INT NOT NULL references composer(id),"
                   "poet_or_lyricist CHAR(100),"
                   "larger_Work_original_publication TEXT,"
                   "poets_associated_movements_or_isms_or_groups TEXT,"
                   "musical_form TEXT,"
                   "first_line TEXT,"
                   "year_of_composition CHAR(50),"
                   "catalog_designation TEXT,"
                   "complete_edition_reference TEXT,"
                   "composer_place_of_birth CHAR(100),"
                   "original_language CHAR(100),"
                   "voice_part_suggested_by_the_composer TEXT,"
                   "Berton_Coffin_suggested_voice_type TEXT,"
                   "Berton_Coffin_suggested_song_type TEXT,"
                   "piano_and_voice_only CHAR(100),"
                   "orchestra_and_voice CHAR(100),"
                   "other_instrumentation_and_voice TEXT,"
                   "more_than_one_voice TEXT,"
                   "original_key TEXT,"
                   "range_in_original_key TEXT,"
                   "dedicated_to CHAR(100),"
                   "premiered_by CHAR(100),"
                   "commissioned_by CHAR(100),"
                   "average_duration CHAR(50),"
                   "degrees_location_relationship_occupation TEXT,"
                   "recommended_printed_source TEXT,"
                   "recommended_translation_source TEXT,"
                   "score_source TEXT,"
                   "audio_source TEXT,"
                   "other_musical_allusions_accompaniment_figures TEXT,"
                   "contributor CHAR(100),"
                   "orderID CHAR(50)"
                   ")")

    cursor.execute("CREATE TABLE composers("
                   "id SERIAL PRIMARY KEY,"
                   "composer TEXT NOT NULL"
                   ")")

    cursor.execute("CREATE TABLE features("
                   "id SERIAL PRIMARY KEY,"
                   "name TEXT NOT NULL"
                   ")")

    cursor.execute("CREATE TABLE keywords("
                   "id SERIAL PRIMARY KEY,"
                   "name TEXT NOT NULL"
                   ")")

    cursor.execute("CREATE TABLE mappingFeatureToSong("
                   "song_id INT NOT NULL references songs(id),"
                   "feature_id INT NOT NULL references features(id)"
                   ")")

    cursor.execute("CREATE TABLE mappingKeywordToSong("
                   "song_id INT NOT NULL references songs(id),"
                   "keyword_id INT NOT NULL references keywords(id)"
                   ")")



    mappingDBColumnToJSONKeys = [{"song_title": "Song Title"},
                                 {"composer": "Composer"},
                                 {"poet_or_lyricist": "Poet or lyricist"},
                                 {"features": "Features"},
                                 {"keywords": "Keywords"},
                                 {"larger_Work_original_publication": "Larger Work (original publication)"},
                                 {"poets_associated_movements_or_isms_or_groups": "Poet's associated movements or -isms or Groups"},
                                 {"musical_form": "Musical form"},
                                 {"first_line": "First Line"},
                                 {"year_of_composition": "Year of Composition"},
                                 {"catalog_designation": "Catalog designation"},
                                 {"complete_edition_reference": "Complete edition Reference"},
                                 {"composer_place_of_birth": "Composer's Place of Birth"},
                                 {"original_language": "Original Language"},
                                 {"voice_part_suggested_by_the_composer": "Voice part suggested by the composer (where possible taken from the indication or publication)"},
                                 {"Berton_Coffin_suggested_voice_type": "Berton Coffin suggested voice type"},
                                 {"Berton_Coffin_suggested_song_type": "Berton Coffin suggested song type"},
                                 {"piano_and_voice_only": "Piano and voice only (Yes or leave blank)"},
                                 {"orchestra_and_voice": "Orchestra and voice (check box)"},
                                 {"other_instrumentation_and_voice": "Other instrumentation and voice (check box)"},
                                 {"more_than_one_voice": "More than one voice"},
                                 {"original_key": "Original Key"},
                                 {"range_in_original_key": "Range in original key"},
                                 {"dedicated_to": "Dedicated to"},
                                 {"premiered_by": "Premiered by"},
                                 {"commissioned_by": "Commissioned by"},
                                 {"average_duration": "Average Duration"},
                                 {"degrees_location_relationship_occupation": "Degrees (location, relationship, occupation)"},
                                 {"recommended_printed_source": "Recommended Printed Source"},
                                 {"recommended_translation_source": "Recommended Translation Source"},
                                 {"score_source": "Score source"},
                                 {"audio_source": "Audio source"},
                                 {"other_musical_allusions_accompaniment_figures": "Other (musical allusions, accompaniment figures)"},
                                 {"contributor": "Contributor"},
                                 {"orderID": "order"}]

    # what does this line mean: ??
    cursor.execute("select relname from pg_class where relkind='r' and relname !~ '^(pg_|sql_)';")
    print(cursor.fetchall())

    insertStatement = []
    i = 0
    for aSong in songdata:
        insertStatement.append(i)

        for aMapping in mappingDBColumnToJSONKeys:
            theValue = list(aMapping.values())[0]

            songElement = 'null'
            if theValue in aSong:
                songElement = aSong.get(theValue)

            insertStatement.append(songElement)
            #
            # for key, value in song.items():
            #     val = value
            #     if not value:
            #         val = 'null'
            #     insertStatement.append(val)

        insertTuple = tuple(insertStatement)
        print(insertTuple)

        sql = "INSERT INTO songs VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
        cursor.execute(sql, insertTuple)

        insertStatement = []
        i = i + 1

    conn.commit()

    seeDataInDB(cursor)
