#include <bits/stdc++.h>
#include<string.h>
#include<stdlib.h>
#include<string>
#include<sstream>
#include<iostream>
#include <algorithm>
#include <cctype>
#include "Tokenizer.h"
#include<locale>
using namespace std;
int i,j,k,m,n;
int hitcount=0,misscount=0;
typedef struct attribute{
	char name[100];
	char type[100];
}attribute;
typedef struct page{
	int StartingRecordId;
	int EndingRecordId;
	char * data;
	int no_in_db;
	int pos;
}page;
typedef struct table{
	char table_name[100];
	vector <attribute *> attributes_of_table;
	vector <page *> list_of_pages;
}table;
int counter=0;



inline bool caseInsCharCompareN(char a, char b) {
	return(toupper(a) == toupper(b));
}
bool caseInsCompare(const string& s1, const string& s2) {
	return((s1.size() == s2.size()) &&
			equal(s1.begin(), s1.end(), s2.begin(), caseInsCharCompareN));
}



//Declare Tokenizer
void print_vec(vector <string> v)
{
	for(int i=0;i<v.size();i++)
	{
		cout << v[i] << " ";
	}
	cout << endl;
}








Tokenizer::Tokenizer() : buffer(""), token(""), delimiter(DEFAULT_DELIMITER)
{
	currPos = buffer.begin();
}
Tokenizer::Tokenizer(const std::string& str, const std::string& delimiter) : buffer(str), token(""), delimiter(delimiter)
{
	currPos = buffer.begin();
}
Tokenizer::~Tokenizer()
{
}
void Tokenizer::set(const std::string& str, const std::string& delimiter)
{
	this->buffer = str;
	this->delimiter = delimiter;
	this->currPos = buffer.begin();
}
void Tokenizer::setString(const std::string& str)
{
	this->buffer = str;
	this->currPos = buffer.begin();
}
void Tokenizer::setDelimiter(const std::string& delimiter)
{
	this->delimiter = delimiter;
	this->currPos = buffer.begin();
}
std::string Tokenizer::next()
{
	if(buffer.size() <= 0) return "";           // skip if buffer is empty
	token.clear();                              // reset token string
	this->skipDelimiter();                      // skip leading delimiters
	while(currPos != buffer.end() && !isDelimiter(*currPos))
	{
		token += *currPos;
		++currPos;
	}
	return token;
}
void Tokenizer::skipDelimiter()
{
	while(currPos != buffer.end() && isDelimiter(*currPos))
		++currPos;
}
bool Tokenizer::isDelimiter(char c)
{
	return (delimiter.find(c) != std::string::npos);
}
std::vector<std::string> Tokenizer::split()
{
	std::vector<std::string> tokens;
	std::string token;
	while((token = this->next()) != "")
	{
		tokens.push_back(token);
	}
	return tokens;
}








//End Tokenizer Declaration

















class DBSystem 
{
	public:
		map <string,int> map_tables;
		map <int,int> map_pages;
		map<string,int> rec_counts;
		map<string,int> unique_counts;
		char ** db;
		vector <table *> table_list;
		vector <page *> page_list;
		int page_size;
		int num_pages;
		char file_path[200];
		int * tables;
		int * mod_tables;
		int * pages;
		int db_count;
		int warning_yes;
		int warning_no;
		list <int>  db_queue;
		int * modified;
		int * db_compare_flag;
		char conf_store[200];
		string conf_path;
		int hmflag = 0;
		void readConfig(string configFilePath) 
		{
			char * input_file = new char[configFilePath.size() + 1];
			copy(configFilePath.begin(), configFilePath.end(),input_file);
			input_file[configFilePath.size()] = '\0';
			char * input = strcat(input_file,"config.txt");
			conf_path = input;
			ifstream infile(input);
			string line;
			int table_flag = 0;
			int attribute_flag = 0;
			int count_tables = 0;
			db_count=0;warning_yes=0;warning_no=0;
			while(getline(infile, line))
			{
				char * str = new char[line.length()+1];
				strcpy(str,line.c_str());
				vector <char *> token_list;
				char * token = strtok(str, " ");
				while(token!=NULL)
				{
					token_list.push_back(token);
					token = strtok(NULL, " ");
				}
				if(strcmp(token_list[0],"END")==0)
				{
					table_flag = 0;
					attribute_flag = 0;
					continue;
				}
				if(table_flag == 1)
				{
					if(attribute_flag == 1)
					{
						char * att = strtok(token_list[0],",");
						attribute * a = (attribute *)malloc(sizeof(attribute));
						strcpy(a->name,att);
						att = strtok(NULL,",");
						strcpy(a->type,att);
						table * t = table_list.back();
						t->attributes_of_table.push_back(a);
					}
					if(attribute_flag == 0)
					{
						mod_tables[count_tables] = 0;
						map_tables[token_list[0]] = count_tables++;
						table * t = (table *)malloc(sizeof(table));
						strcpy(t->table_name,token_list[0]);
						attribute_flag = 1;
						table_list.push_back(t);
					}
				}
				else
				{
					for(i=0;i<token_list.size();i++)
					{
						if(strcmp("PAGE_SIZE",token_list[i])==0)
						{
							page_size = atoi(token_list[i+1]);
							break;
						}
						if(strcmp("NUM_PAGES",token_list[i])==0)
						{
							num_pages = atoi(token_list[i+1]);
							break;
						}
						if(strcmp("PATH_FOR_DATA",token_list[i])==0)
						{
							strcpy(file_path,token_list[i+1]);
							break;
						}
						if(strcmp("BEGIN",token_list[i])==0)
						{
							table_flag = 1;
							attribute_flag = 0;
						}
					}
				}
			}
			db = (char **)malloc(sizeof(char*)*num_pages);
			for(i=0;i<num_pages;i++)
			{
				db[i] = (char *)malloc(sizeof(char)*page_size);
			}
			modified = (int *)malloc(sizeof(int)*1000000);
			mod_tables = (int *)malloc(sizeof(int)*1000000);
			for(i=0;i<num_pages;i++)
			{
				modified[i]=0;
			}
			tables = (int *)malloc(sizeof(int)*num_pages);
			pages = (int *)malloc(sizeof(int)*num_pages);
			for(i=0;i<num_pages;i++)
			{
				db_queue.push_back(i);
				tables[i] = 0;
				pages[i] = 0;
			}
			db_compare_flag = (int *)malloc(sizeof(int)*num_pages);
			for(i=0;i<num_pages;i++)
			{
				db_compare_flag[i] = 0;
			}
		}

		void populatePageInfo() 
		{
			for(i=0;i<table_list.size();i++)
			{
				int record_count = 0;
				char * name = table_list[i]->table_name;
				char path_for_table[300] = "\0";
				strcat(path_for_table,file_path);
				strcat(path_for_table,"/");
				strcat(path_for_table,name);
				strcat(path_for_table,".csv");
				ifstream inp_table(path_for_table);
				string l;
				page * current_page = (page *)malloc(sizeof(page));
				page_list.push_back(current_page);
				current_page->pos = page_list.size()-1;
				current_page->data = (char *)malloc(sizeof(char) * page_size);
				current_page->data[0] = '\0';
				current_page -> StartingRecordId = record_count;
				char page_data[page_size];
				page_data[0] = '\0';
				while(getline(inp_table,l))
				{
					Tokenizer tk;
					tk.set(l);
					tk.setDelimiter(",");
					string t;
					string t_name = name;
					vector <attribute *> datatypes = table_list[i]->attributes_of_table;
					int cntr = 0;
					while((t=tk.next())!="")
					{
						string s = t_name + " " + t;
						if(rec_counts.find(s) == rec_counts.end())
						{
							string s1 = t_name + " " + datatypes[cntr]->name;
							if(unique_counts.find(s1) == unique_counts.end())
								unique_counts[s1] = 1;
							else
								unique_counts[s1]++;
							rec_counts[s]  = 1;
						}
						cntr++;
					}
					char temp[1000];
					temp[0] = '\0';
					char record_count_string[10];
					sprintf(record_count_string,"%d",record_count);
					strcat(temp,record_count_string);
					strcat(temp,",");
					char * lin = new char[l.length()+1];
					strcpy(lin,l.c_str());
					strcat(temp,lin);
					strcat(temp,",");
					strcat(temp,"|");
					if((strlen(temp)+strlen(page_data))<page_size)
					{
						strcat(page_data,temp);
					}
					else
					{
						current_page->EndingRecordId = record_count-1;
						current_page->no_in_db = -1;
						strcpy(current_page->data,page_data);
						table_list[i]->list_of_pages.push_back(current_page);
						page_data[0] = '\0';
						current_page = (page *)malloc(sizeof(page));
						page_list.push_back(current_page);
						current_page->pos = page_list.size()-1;
						current_page->data = (char *)malloc(sizeof(char) * page_size);
						current_page->data[0] = '\0';
						current_page -> StartingRecordId = record_count;
						strcat(page_data,temp);
					}
					record_count++;
				}
				current_page->EndingRecordId = record_count-1;
				current_page->no_in_db = -1;
				strcpy(current_page->data,page_data);
				table_list[i]->list_of_pages.push_back(current_page);
			}
		}

		void printQueue()
		{
			cout << "Queue" << " -- ";
			const list<int>& s = db_queue;
			list<int>::const_iterator i;
			for( i = s.begin(); i != s.end(); ++i)
				cout << *i << " ";
			cout << endl;
		}

		void printDB()
		{
			for(i=0;i<num_pages;i++)
				cout << db[i] << endl;
		}

		string getRecord(string tableName, int recordId) 
		{
			int table_id = map_tables[tableName];
			char fpage[page_size];
			fpage[0] = '\0';
			int page_location;
			char page_with_record[page_size];
			fpage[0] = '\0';
			string record;
			mod_tables[table_id]=1;
			for(i=0;i<table_list[table_id]->list_of_pages.size();i++)
			{
				if(table_list[table_id]->list_of_pages[i]->StartingRecordId<=recordId && table_list[table_id]->list_of_pages[i]->EndingRecordId>=recordId)
				{
					if(table_list[table_id]->list_of_pages[i]->no_in_db == -1)
					{
						strcpy(fpage,table_list[table_id]->list_of_pages[i]->data);
						misscount++;
						int position = findPosition();
						if(hmflag==0)
							cout << "MISS " << position << endl;
						table_list[table_id]->list_of_pages[i]->no_in_db = position;
						tables[position]=table_id;
						pages[position]=i;
						//Moving to LRU Cache
						strcpy(db[position],fpage);
						//Access from cache
						strcpy(page_with_record,db[position]);
						map_pages[position] = table_list[table_id]->list_of_pages[i]->pos;
						db_queue.remove(position);
						db_queue.push_back(position);

					}
					else
					{
						hitcount++;
						if(hmflag==0)
							cout << "HIT" << endl;
						//Directly access cache
						strcpy(page_with_record,table_list[table_id]->list_of_pages[i]->data);
						db_queue.remove(table_list[table_id]->list_of_pages[i]->no_in_db);
						db_queue.push_back(table_list[table_id]->list_of_pages[i]->no_in_db);
					}
					break;
				}
			}
			char * token = strtok(page_with_record,"|");
			while(token!=NULL)
			{
				char store[page_size];
				strcpy(store,token);
				string fin(store,strlen(store));
				vector <string> tok;
				string buf;
				stringstream ss(fin);
				while(getline(ss,buf,','))
				{
					tok.push_back(buf);
				}
				if(atoi(tok[0].c_str()) == recordId)
					return fin;
				token = strtok(NULL,"|");
			}
		}

		void insertRecord(string tableName,string record)
		{
			char * to_add;
			char temp[page_size];
			temp[0] = '\0';
			strcat(temp,record.c_str());
			int table_id = map_tables[tableName];
			Tokenizer tk;
			tk.set(record);
			tk.setDelimiter(",");
			string t;
			string t_name = tableName;
			vector <attribute *> datatypes = table_list[table_id]->attributes_of_table;
			int cntr = 0;
			while((t=tk.next())!="")
			{
				string s = t_name + " " + t;
				if(rec_counts.find(s) == rec_counts.end())
				{
					string s1 = t_name + " " + datatypes[cntr]->name;
					if(unique_counts.find(s1) == unique_counts.end())
						unique_counts[s1] = 1;
					else
						unique_counts[s1]++;
					rec_counts[s]  = 1;
				}
				cntr++;
			}
			mod_tables[table_id]=1;
			int last_rec_num = table_list[table_id]->list_of_pages.back()->EndingRecordId;
			char * token;
			char rec[page_size];
			rec[0] = '\0';
			char rec_id_string[20];
			sprintf(rec_id_string,"%d",last_rec_num+1);
			strcat(rec,rec_id_string);
			strcat(rec,",");
			int position = -1;
			strcat(rec,temp);
			strcat(rec,",");
			strcat(rec,"|");
			if(table_list[table_id]->list_of_pages.back()->no_in_db!=-1)
			{
				to_add = db[table_list[table_id]->list_of_pages.back()->no_in_db];
				db_queue.remove(table_list[table_id]->list_of_pages.back()->no_in_db);
				db_queue.push_back(table_list[table_id]->list_of_pages.back()->no_in_db);
			}
			else
			{
				position = findPosition();
				table_list[table_id]->list_of_pages.back()->no_in_db = position;
				tables[position] = table_id;
				pages[position] = table_list[table_id]->list_of_pages.size()-1;
				strcpy(db[position],table_list[table_id]->list_of_pages.back()->data);
				to_add = db[position];
				map_pages[position] = table_list[table_id]->list_of_pages.back()->pos;
				db_queue.remove(position);
				db_queue.push_back(position);
			}
			if(strlen(to_add) + strlen(rec) < page_size)
			{
				table_list[table_id]->list_of_pages.back()->EndingRecordId++;
				modified[table_list[table_id]->list_of_pages.back()->pos] = 1;
				strcat(to_add,rec);
			}
			else
			{
				if(position!=-1)
				{
					page_list[pages[position]]->no_in_db = -1;
				}
				page * new_page = (page *)malloc(sizeof(page));
				new_page -> data = (char *)malloc(sizeof(char)*page_size);
				new_page->data[0] = '\0';
				page_list.push_back(new_page);
				new_page->pos = page_list.size()-1;
				modified[page_list.size()-1] = 1;
				new_page->StartingRecordId = last_rec_num+1;
				new_page->EndingRecordId = last_rec_num+1;
				strcpy(new_page->data,rec);
				if(position == -1)
					position = findPosition();
				db[position][0] = '\0';
				strcpy(db[position],rec);
				map_pages[position] = table_list[table_id]->list_of_pages.back()->pos;
				db_queue.remove(position);
				db_queue.push_back(position);
				new_page->no_in_db = position;
				table_list[table_id]->list_of_pages.push_back(new_page);
				tables[position] = table_id;
				pages[position] = table_list[table_id]->list_of_pages.size()-1;
			}
		}

		void flushPages()
		{
			for(i=0;i<page_list.size();i++)
			{
				if(modified[i]==1)
				{
					modified[i] = 0;
					if(page_list[i]->no_in_db!=-1)
					{
						page_list[i]->data[0] = '\0';
						strcpy(page_list[i]->data,db[page_list[i]->no_in_db]);
					}
				}
			}
			for(i=0;i<table_list.size();i++)
			{
				if(mod_tables[map_tables[table_list[i]->table_name]]==1)
				{
					mod_tables[map_tables[table_list[i]->table_name]]=0;
					char filename[1000];
					filename[0] = '\0';
					strcat(filename,table_list[i]->table_name);
					strcat(filename,".csv");
					ofstream outfile;
					outfile.open(filename);
					for(j=0;j<table_list[i]->list_of_pages.size();j++)
					{
						char temporary[page_size];
						temporary[0] = '\0';
						strcpy(temporary,table_list[i]->list_of_pages[j]->data);
						char * token = strtok(temporary,"|");
						while(token!=NULL)
						{
							char line_write[page_size];
							line_write[0] = '\0';
							char store[page_size];
							strcpy(store,token);
							string fin(store,strlen(store));
							vector <string> tok;
							string buf;
							stringstream ss(fin);
							while(getline(ss,buf,','))
							{
								tok.push_back(buf);
							}
							for(k = 1;k<tok.size()-1;k++)
							{
								strcat(line_write,tok[k].c_str());
								strcat(line_write,",");
							}
							strcat(line_write,tok[k].c_str());
							outfile << line_write << endl;
							token = strtok(NULL,"|");
						}

					}
					outfile.close();
				}
			}
		}

		int findPosition()
		{
			int to_remove;
			to_remove = db_queue.front();
			table_list[tables[to_remove]]->list_of_pages[pages[to_remove]]->no_in_db = -1;
			if(warning_no == 0)
			{
				if(modified[map_pages[to_remove]] == 1)
				{
					page_list[map_pages[to_remove]]->data[0] = '\0'; 
					strcpy(page_list[map_pages[to_remove]]->data,db[to_remove]);
				}
			}
			db[to_remove][0] = '\0';
			table_list[tables[to_remove]]->list_of_pages[pages[to_remove]]->no_in_db=-1;
			db_queue.pop_front();
			db_queue.push_back(to_remove);
			return to_remove;
		}


		void print()
		{
			for(i=0;i<table_list.size();i++)
			{
				cout << table_list[i]->table_name << endl;
				cout << table_list[i]->list_of_pages.size() << endl;

				cout << "Number of pages " <<table_list[i]->list_of_pages.size() << endl;
				for(j=0;j<table_list[i]->list_of_pages.size();j++)
				{
					cout << "Starting ID " << table_list[i]->list_of_pages[j]->StartingRecordId << endl;
					cout << "Ending ID " << table_list[i]->list_of_pages[j]->EndingRecordId << endl;
					cout << table_list[i]->list_of_pages[j]->data << endl;
				}
			}
			cout << "Total Pages " << page_list.size() << endl;
		}

		void createCommand(string query)
		{
			Tokenizer str;
			string token;
			str.set(query);
			str.next();
			str.next();
			string tname = str.next();
			str.setDelimiter("(");
			str.next();
			string temp = str.next();
			str.set(temp);
			str.setDelimiter(")");
			string columns =  str.next();
			int flag = 0;
			for(i=0;i<table_list.size();i++)
			{
				if(strcmp(table_list[i]->table_name,tname.c_str()) == 0)
					flag = 1;
			}
			string to_store = "";
			if(flag == 0)
			{
				cout << "Querytype:create" << endl;
				cout <<"Tablename:"<<tname<<endl;
				cout << columns << endl;
				Tokenizer col_tok;
				string col;
				col_tok.set(columns);
				col_tok.setDelimiter(",");
				string data_file = tname + ".data";
				string csv_file = tname + ".csv";
				ofstream tdata;
				tdata.open(data_file.c_str());
				col = col_tok.next();
				while(1)
				{
					Tokenizer atts;
					atts.set(col);
					atts.setDelimiter(" ");
					to_store += atts.next();
					to_store += ":";
					to_store += atts.next();
					if((col = col_tok.next())=="")
						break;
					to_store += ",";
				}
				tdata << to_store;
				tdata.close();
				ofstream tcsv;
				tcsv.open(csv_file.c_str());
				tcsv.close();
				ofstream tconf;
				tconf.open(conf_path.c_str(),std::ios_base::app);
				tconf << "BEGIN" << endl;
				tconf << tname << endl;
				Tokenizer conf_tok;
				string confs;
				conf_tok.set(columns);
				conf_tok.setDelimiter(",");
				confs = conf_tok.next();
				while(1)
				{
					to_store = "";
					Tokenizer atts;
					atts.set(confs);
					atts.setDelimiter(" ");
					to_store += atts.next();
					to_store += ",";
					to_store += atts.next();
					tconf << to_store << endl;
					if((confs = conf_tok.next())=="")
						break;
				}
				tconf << "END" << endl;
				tconf.close();
			}
			else
				cout << "Query Invalid" << endl;
		}



		void selectCommand(string query)
		{





			int invalid = 0;
			int andflag = 1;
			int orflag = 1;
			hmflag=1;
			string temp;
			string orderby = "";
			string groupby = "";
			string having = "";
			string where = "";
			string distinct = "";
			string inner_join1 = "";
			string inner_join2 = "";
			string join_condition = "";
			string join_condition1 = "";
			string inner_join3 = "";
			vector <string> cols_to_select;
			vector < vector<string> > output_of_select;
			vector <string> list_of_joins;
			vector <string> list_of_join_conds;








			Tokenizer toks;
			string token;
			toks.set(query);
			toks.setDelimiter(" ");
			toks.next();
			string toselect = "";
			while(1)
			{
				temp = toks.next();
				string temp1 = temp;
				transform(temp1.begin(), temp1.end(), temp1.begin(),(int (*)(int))tolower);
				if(temp1 == "from")
					break;
				else
				{
					toselect += temp;
				}
			}
			string tables = toks.next();
			int join_flag = 0;
			while((temp = toks.next())!="")
			{
				transform(temp.begin(), temp.end(), temp.begin(),(int (*)(int))tolower);
				if(temp == "where")
					where = toks.next();
				else if(temp == "orderby")
					orderby = toks.next();
				else if(temp == "groupby")
					groupby = toks.next();
				else if(temp == "having")
					having = toks.next();
				else if(temp == "and")
				{
					where += " ";
					where += toks.next();
					andflag = 1;
					orflag = 0;
				}
				else if(temp == "or")
				{
					where += " ";
					where += toks.next();
					orflag = 1;
					andflag = 0;
				}
				else if(temp == "inner" || temp == "join")
				{
					if(join_flag == 0)
					{
						if(temp == "inner")
							toks.next();
						inner_join1 = tables;
						inner_join2 = toks.next();
						list_of_joins.push_back(inner_join1);
						list_of_joins.push_back(inner_join2);
						join_flag = 1;
					}
					else
					{
						if(temp == "inner")
							toks.next();
						inner_join3 = toks.next();
						list_of_joins.push_back(inner_join3);
						join_flag = 2;
					}

				}
				else if(temp == "on")
				{
					if(join_flag == 1)
					{
						join_condition = toks.next();
						list_of_join_conds.push_back(join_condition);
					}
					if(join_flag == 2)
					{
						join_condition1 = toks.next();
						list_of_join_conds.push_back(join_condition1);
					}
				}
			}


			// Add tables and columns
			Tokenizer tabs;
			tabs.set(tables);
			string tab;
			tabs.setDelimiter(",");
			string columns = "";
			int flag = 0;
			while((tab = tabs.next())!="")
			{
				for(i=0;i<table_list.size();i++)
				{
					if(strcmp(table_list[i]->table_name,tab.c_str())==0)
					{
						flag = 1;
						for(j=0;j<table_list[i]->attributes_of_table.size();j++)
						{
							columns += table_list[i]->attributes_of_table[j]->name;
							columns += ",";
						}
					}
				}
			}
			if(flag != 1)
				invalid = 1;











			//Handle Columns
			if(toselect != "*")
			{
				columns = "";
				Tokenizer cols;
				cols.set(toselect);
				cols.setDelimiter(",");
				string col;
				while((col = cols.next())!="")
				{
					string temp_dist = "";
					Tokenizer dist;
					dist.set(col);
					string dis;
					dist.setDelimiter("(");
					if((temp_dist=dist.next()) == "distinct")
					{
						Tokenizer inner;
						inner.set(dist.next());
						inner.setDelimiter(")");
						string t = inner.next();
						distinct += t;
						distinct += ",";
						columns += t;
						columns += ",";
					}
					else
					{
						cols_to_select.push_back(col);
						columns += col;
						columns += ",";
					}
				}
			}











			//Validate groupby and orderby
			if(groupby!="")
			{
				Tokenizer valg;
				valg.set(columns);
				valg.setDelimiter(",");
				string tempg;
				int gflag = 0;
				while((tempg = valg.next())!="")
				{
					if(tempg == groupby)
						gflag = 1;
				}
				if(gflag!=1)
					invalid=1;
			}



			if(join_condition == "" && join_condition1 == "")
			{
				if(invalid!=1)
				{
					int tid = map_tables[tables];
					int number_of_records = table_list[tid]->list_of_pages[table_list[tid]->list_of_pages.size()-1]->EndingRecordId+1;
					vector <attribute *> store_datatype = table_list[tid]->attributes_of_table;
					vector <int> cols_to_output;
					int flag[10000] = {0};







					/////////////////////////////////////////////////////////////////////
					Tokenizer cols;
					cols.set(columns);
					cols.setDelimiter(",");
					string temp;
					while((temp = cols.next())!="")
					{
						for(j=0;j<store_datatype.size();j++)
						{
							if(store_datatype[j]->name == temp)
							{
								cols_to_output.push_back(j);
								flag[j] = 1;
							}
						}
					}









					/////////////////////////////////////////////////////////////////////
					string store_conds[store_datatype.size()][2];
					map<string,int> col_to_num;
					map<int,string> num_to_col;
					for(j=0;j<store_datatype.size();j++)
					{
						store_conds[j][0] = "-";
						store_conds[j][1] = "-";
						col_to_num[store_datatype[j]->name] = j;
						num_to_col[j] = store_datatype[j]->name;
					}



					/////////////////////////////////////////////////////////////////////
					Tokenizer conds;
					conds.set(where);
					conds.setDelimiter(" ");
					string temp1;
					int total_conds = 0;
					while((temp1 = conds.next())!="")
					{
						total_conds+=1;
						if(temp1.find(">=") < temp1.length())
						{
							store_conds[col_to_num[temp1.substr(0,temp1.find(">="))]][0] = ">=";
							store_conds[col_to_num[temp1.substr(0,temp1.find(">="))]][1] = temp1.substr(temp1.find(">=")+2,temp1.length());
						}
						else if(temp1.find("<=") < temp1.length())
						{
							store_conds[col_to_num[temp1.substr(0,temp1.find("<="))]][0] = "<=";
							store_conds[col_to_num[temp1.substr(0,temp1.find("<="))]][1] = temp1.substr(temp1.find("<=")+2,temp1.length());
						}
						else if(temp1.find("!=") < temp1.length())
						{
							store_conds[col_to_num[temp1.substr(0,temp1.find("!="))]][0] = "!=";
							store_conds[col_to_num[temp1.substr(0,temp1.find("!="))]][1] = temp1.substr(temp1.find("!=")+2,temp1.length());
						}
						else if(temp1.find("<") < temp1.length())
						{
							store_conds[col_to_num[temp1.substr(0,temp1.find("<"))]][0] = "<";
							store_conds[col_to_num[temp1.substr(0,temp1.find("<"))]][1] = temp1.substr(temp1.find("<")+1,temp1.length());
						}
						else if(temp1.find(">") < temp1.length())
						{
							store_conds[col_to_num[temp1.substr(0,temp1.find(">"))]][0] = ">";
							store_conds[col_to_num[temp1.substr(0,temp1.find(">"))]][1] = temp1.substr(temp1.find(">")+1,temp1.length());
						}
						else if(temp1.find("LIKE") < temp1.length())
						{
							store_conds[col_to_num[temp1.substr(0,temp1.find("LIKE"))]][0] = "LIKE";
							store_conds[col_to_num[temp1.substr(0,temp1.find("LIKE"))]][1] = temp1.substr(temp1.find("LIKE")+4,temp1.length());
						}
						else if(temp1.find("=") < temp1.length())
						{
							store_conds[col_to_num[temp1.substr(0,temp1.find("="))]][0] = "=";
							store_conds[col_to_num[temp1.substr(0,temp1.find("="))]][1] = temp1.substr(temp1.find("=")+1,temp1.length());
						}
					}

					int validation = 0;
					for(j=0;j<store_datatype.size();j++)
					{
						if(toselect=="*")
							cols_to_select.push_back(store_datatype[j]->name);
						if(store_conds[j][0] != "-")
						{
							string in = "int";
							string fl = "float";
							if(store_datatype[j]->type == in)
							{
								for(k=0;k<store_conds[j][1].length();k++)
								{
									if(store_conds[j][1][k] >='0' && store_conds[j][1][k] <= '9')
									{}
									else
									{
										cout << "Invalid Operator <" << store_conds[j][1] << "> for datatype <" << store_datatype[j]->type << ">" << endl;
										return;
									}
								}
							}
							if(store_datatype[j]->type == fl)
							{
								for(k=0;k<store_conds[j][1].length();k++)
								{
									if((store_conds[j][1][k] >='0' && store_conds[j][1][k] <= '9') || store_conds[j][1][k]=='.')
									{}
									else
									{
										cout << "Invalid Operator <" << store_conds[j][1] << "> for datatype <" << store_datatype[j]->type << ">" << endl;
										return;
									}
								}
							}
						}
					}
					vector < vector<string> > final_to_print;
					for(j=0;j<number_of_records;j++)
					{
						Tokenizer rec_tok;
						rec_tok.set(getRecord(tables,j));
						rec_tok.setDelimiter(",");
						string rec_temp;
						int counter=0;
						int pflag = 0;
						vector <string> final_to_print;
						while((rec_temp = rec_tok.next())!="")
						{
							if(counter>0)
							{
								final_to_print.push_back(rec_temp);
								if(store_conds[counter-1][0]!="-")
								{
									string fl = "float";
									string in = "int";
									string st = "string";
									if(store_datatype[counter-1]->type==fl || store_datatype[counter-1]->type==in)
									{
										if(store_conds[counter-1][0] == ">=")
										{
											if(stof(rec_temp.substr(1,rec_temp.length()-2))>=stof(store_conds[counter-1][1]))
											{
												pflag+=1;
											}
										}
										if(store_conds[counter-1][0] == "<=")
										{
											if(stof(rec_temp.substr(1,rec_temp.length()-2))<=stof(store_conds[counter-1][1]))
											{
												pflag+=1;
											}
										}
										if(store_conds[counter-1][0] == "=")
										{
											if(stof(rec_temp.substr(1,rec_temp.length()-2))==stof(store_conds[counter-1][1]))
											{
												pflag+=1;
											}
										}
										if(store_conds[counter-1][0] == "!=")
										{
											if(stof(rec_temp.substr(1,rec_temp.length()-2))!=stof(store_conds[counter-1][1]))
											{
												pflag+=1;
											}
										}
										if(store_conds[counter-1][0] == ">")
										{
											if(stof(rec_temp.substr(1,rec_temp.length()-2))>stof(store_conds[counter-1][1]))
											{
												pflag+=1;
											}
										}
										if(store_conds[counter-1][0] == "<")
										{
											if(stof(rec_temp.substr(1,rec_temp.length()-2))<stof(store_conds[counter-1][1]))
											{
												pflag+=1;
											}
										}
									}
									else if(store_datatype[counter-1]->type==st)
									{
										if(store_conds[counter-1][0] == "LIKE")
										{
											if(caseInsCompare(rec_temp.substr(1,rec_temp.length()-2),store_conds[counter-1][1]))
											{
												pflag+=1;
											}
										}
										if(store_conds[counter-1][0] == "=")
										{
											if(rec_temp.substr(1,rec_temp.length()-2)==store_conds[counter-1][1])
											{
												pflag+=1;
											}
										}
									}
								}
							}
							counter++;
						}
						if((pflag==total_conds && andflag==1) || (orflag==1 && pflag>=1) || (where==""))
						{
							output_of_select.push_back(final_to_print);
						}
					}

					int data[10000];
					for(i=0;i<output_of_select.size();i++)
						data[i]=i;
					if(orderby!="")
					{
						int orderflag[100000]={0};
						int ascflag = 0;
						vector <string> order_cols;
						vector <string> order_cols_type;
						if(orderby.find("(ASC)")<orderby.length())
							ascflag=1;
						orderby = orderby.substr(0,orderby.find("("));
						Tokenizer ord;
						ord.set(orderby);
						ord.setDelimiter(",");
						string temp2;
						while((temp2 = ord.next())!="")
						{
							order_cols.push_back(temp2);
							order_cols_type.push_back(store_datatype[col_to_num[temp2]]->type);
							orderflag[col_to_num[temp2]] = 1;
						}


/*


						int j = 0;
						int tmp = 0;
						for(int i=0;i<output_of_select.size();i++){
							j = i;
							for(int k = i;k<output_of_select.size();k++){
								if(rec_compare(output_of_select[data[j]],output_of_select[data[k]],order_cols,order_cols_type,col_to_num)){
									j = k;
								}
							}
							tmp = data[i];
							data[i] = data[j];
							data[j] = tmp;
						}
						if(ascflag==0)
						{
							for(i=0;i<output_of_select.size()/2;i++)
							{
								int temp = data[i];
								data[i] = data[output_of_select.size()-i-1];
								data[output_of_select.size()-i-1] = temp;
							}
						}
						for(i=0;i<output_of_select.size();i++)
						{
							cout << "[ ";
							for(j=0;j<output_of_select[data[i]].size();j++)
							{
								if(flag[j]==1)
									cout << output_of_select[data[i]][j] << " ";
							}
							cout << "]";
							cout << endl;
						}*/

							vector <string> to_pr = phase_merge(output_of_select,order_cols,order_cols_type,col_to_num,ascflag);
							for(j=0;j<cols_to_select.size();j++)
							{
							cout << "\"" << cols_to_select[j] << "\"";
							if(j!=cols_to_select.size()-1)
							cout << ",";
							}
							cout << endl;
							for(j=0;j<to_pr.size();j++)
							{
							Tokenizer an;
							an.set(to_pr[j]);
							an.setDelimiter(",");
							string te;
							int counter =0;
							vector <string> each_record;
							while((te=an.next())!="")
							{
							each_record.push_back(te);
							}
							if(toselect!="*")
							{
							for(k=0;k<cols_to_select.size();k++)
							{
							cout << each_record[col_to_num[cols_to_select[k]]];
							if(k!=cols_to_select.size()-1)
							cout <<",";
							}

							}
							else
							{
							for(k=0;k<each_record.size();k++)
							{
							cout << each_record[k];
							if(k!=each_record.size()-1)
							cout << ",";
							}
							}
							cout << endl;
							}
					}




					if(orderby=="")
					{
						for(j=0;j<cols_to_select.size();j++)
						{
							cout << "\"" <<  cols_to_select[j] << "\"";
							if(j!=cols_to_select.size()-1)
								cout << ",";
						}
						cout << endl;

						for(i=0;i<output_of_select.size();i++)
						{
							for(j=0;j<cols_to_select.size();j++)
							{
								string t = cols_to_select[j];
								int n = col_to_num[t];
								cout << output_of_select[i][n];
								if(j!=cols_to_select.size()-1)
									cout << ",";
							}
							cout << endl;
						}
					}
				}
				else
					cout << "Query Invalid" << endl;
			}
			else if(join_condition != "" && join_condition1 == "")
			{

				//FOR JOIN COMMANDS
				int join_invalid = 0;
				string col_in_t1;
				string col_in_t2;
				Tokenizer cond_tok;
				cond_tok.set(join_condition);
				cond_tok.setDelimiter("=");
				string temp;
				string col_join1,col_join2;
				while((temp=cond_tok.next())!="")
				{
					Tokenizer tabncol;
					tabncol.set(temp);
					tabncol.setDelimiter(".");
					string temp3 = tabncol.next();
					if(temp3 == inner_join1)
						col_join1 = tabncol.next();
					else if(temp3 == inner_join2)
						col_join2 = tabncol.next();
					else
						join_invalid = 1;
				}











				if(join_invalid != 1)
				{
					int tid1 = map_tables[inner_join1];
					int tid2 = map_tables[inner_join2];
					int nrecords1 = table_list[tid1]->list_of_pages[table_list[tid1]->list_of_pages.size()-1]->EndingRecordId+1;
					int nrecords2 = table_list[tid2]->list_of_pages[table_list[tid2]->list_of_pages.size()-1]->EndingRecordId+1;
					vector <attribute *> att1 = table_list[tid1]->attributes_of_table;
					vector <attribute *> att2 = table_list[tid2]->attributes_of_table;
					vector < vector<string> > out1;
					vector < vector<string> > out2;
					vector <string> order_col1;
					order_col1.push_back(col_join1);
					vector <string> order_col2;
					order_col2.push_back(col_join2);
					vector <string> order_col_type1;
					vector <string> order_col_type2;
					string col_join1_type;
					string col_join2_type;
					map <string,int> col_to_num1;
					map <string,int> col_to_num2;
					for(int i=0; i<att1.size();i++)
					{
						col_to_num1[att1[i]->name] = i;
						if(att1[i]->name == col_join1)
						{
							col_join1_type = att1[i]->type;
						}
					}
					for(int i=0;i<att2.size();i++)
					{
						col_to_num2[att2[i]->name] = i;
						if(att2[i]->name == col_join2)
							col_join2_type = att2[i]->type;
					}
					order_col_type1.push_back(col_join1_type);
					order_col_type2.push_back(col_join2_type);
					for(int i=0;i<nrecords1;i++)
					{
						int countr = 0;
						Tokenizer rec_tok;
						rec_tok.set(getRecord(inner_join1,i));
						rec_tok.setDelimiter(",");
						string rec_temp;
						vector <string> final_to_print;
						while((rec_temp = rec_tok.next())!="")
						{
							if(countr>0)
							{
								final_to_print.push_back(rec_temp);
							}
							countr=1;
						}
						out1.push_back(final_to_print);
					}
					for(int i=0;i<nrecords2;i++)
					{
						int countr = 0;
						Tokenizer rec_tok;
						rec_tok.set(getRecord(inner_join2,i));
						rec_tok.setDelimiter(",");
						string rec_temp;
						vector <string> final_to_print;
						while((rec_temp = rec_tok.next())!="")
						{
							if(countr>0)
							{
								final_to_print.push_back(rec_temp);
							}
							countr=1;
						}
						out2.push_back(final_to_print);
					}
					vector <string> sorted1 = phase_merge(out1,order_col1,order_col_type1,col_to_num1,1);
					vector < vector<string> > store_sorted1;
					for(int i=0;i<sorted1.size();i++)
					{
						Tokenizer tk;
						tk.set(sorted1[i]);
						tk.setDelimiter(",");
						string temp;
						vector<string> temp_vec;
						while((temp=tk.next())!="")
						{
							temp_vec.push_back(temp);
						}
						store_sorted1.push_back(temp_vec);
					}

					vector <string> sorted2 = phase_merge(out2,order_col2,order_col_type2,col_to_num2,1);
					vector < vector<string> > store_sorted2;
					for(int i=0;i<sorted2.size();i++)
					{
						Tokenizer tk;
						tk.set(sorted2[i]);
						tk.setDelimiter(",");
						string temp;
						vector<string> temp_vec;
						while((temp=tk.next())!="")
						{
							temp_vec.push_back(temp);
						}
						store_sorted2.push_back(temp_vec);
					}
					vector < vector<string> > an = merge_join(store_sorted1,col_to_num1[order_col1[0]],store_sorted2,col_to_num2[order_col2[0]],order_col_type1[0]);

					vector <int> print_order;

					for(int i=0;i<cols_to_select.size();i++)
					{
						Tokenizer tk;
						tk.set(cols_to_select[i]);
						tk.setDelimiter(".");
						string temp = tk.next();
						string temp1 = tk.next();
						if(temp == inner_join1)
							print_order.push_back(col_to_num1[temp1]);
						else
							print_order.push_back(col_to_num2[temp1] + att1.size());
					}
					for(int i=0;i<an.size();i++)
					{
						cout << "[ ";
						for(int j=0;j<print_order.size();j++)
						{
							cout << an[i][print_order[j]] << " ";
						}
						cout << " ]";
						cout << endl;
					}



				}
				else
					cout << "Query Invalid" << endl;
			}
			else
			{
				string t11,t12,t21,t22;
				string c11,c12,c21,c22;
				string common_table;
				Tokenizer tk;
				tk.set(join_condition);
				tk.setDelimiter("=");
				string temp1,temp2;
				temp1 = tk.next();
				temp2 = tk.next();
				Tokenizer tok;
				tok.set(temp1);
				tok.setDelimiter(".");
				t11 = tok.next();
				c11 = tok.next();
				tok.set(temp2);
				tok.setDelimiter(".");
				t12 = tok.next();
				c12 = tok.next();
				tk.set(join_condition1);
				tk.setDelimiter("=");
				temp1 = tk.next();
				temp2 = tk.next();
				tok.set(temp1);
				tok.setDelimiter(".");
				t21 = tok.next();
				c21 = tok.next();
				tok.set(temp2);
				tok.setDelimiter(".");
				t22 = tok.next();
				c22 = tok.next();
				common_table = inner_join1;
				string other1,other2,other_col1,other_col2,col1,col2;
				if(t11!=common_table)
				{
					other1 = t11;
					other_col1 = c11;
					col1 = c12;
				}
				else
				{
					other1 = t12;
					other_col1 = c12;
					col1 = c11;
				}
				if(t21!=common_table)
				{
					other2 = t21;
					other_col2 = c21;
					col2 = c22;
				}
				else
				{
					other2 = t22;
					other_col2 = c22;
					col2 = c21;
				}
				int weight1 = join_weight(common_table,col1,other1,other_col1);
				int weight2 = join_weight(common_table,col2,other2,other_col2);

				if(weight1 < weight2)
				{
					cout << "((" << other1 << "," << common_table << ")," << other2 << ")" << endl;
					cout << weight1*weight2 << endl;
				}
				else
				{
					cout << "(" << other1 << ",(" << common_table << "," << other2 << "))" << endl;
					cout << weight2*weight1 << endl;
				}
			}
			hmflag = 0;

		}


		int join_weight(string table1,string col1,string table2,string col2)
		{
			int blocks1 = table_list[map_tables[table1]]->list_of_pages.size();
			int blocks2 = table_list[map_tables[table2]]->list_of_pages.size();
			int count1 = table_list[map_tables[table1]]->list_of_pages[table_list[map_tables[table1]]->list_of_pages.size()-1]->EndingRecordId+1;
			int count2 = table_list[map_tables[table2]]->list_of_pages[table_list[map_tables[table2]]->list_of_pages.size()-1]->EndingRecordId+1;
			int unique1 = v(table1,col1);
			int unique2 = v(table2,col2);
			if(unique1 == 0)
				unique1 =1;
			if(unique2 == 0)
				unique2 = 1;
			if(unique1<unique2)
				return blocks1 + blocks2 + (((count1/unique1) * (count2/unique2) * unique1)/2);
			else
				return blocks1 + blocks2 + (((count1/unique1) * (count2/unique2) * unique2)/2);
		}




		vector < vector<string> > merge_join(vector < vector<string> > first, int col1, vector < vector<string> > second, int col2, string type)
		{
			string in = "int";
			string fl = "float";
			string st = "string";
			vector < vector<string> > to_return;
			if(type==in || type==fl)
			{
				int counter1 = 0;
				int counter2 = 0;
				while(1)
				{
					if(counter1>=first.size() || counter2>=second.size())
						break;
					float a = stof(first[counter1][col1].substr(1,first[counter1][col1].length()-2));
					float b = stof(second[counter2][col2].substr(1,second[counter2][col2].length()-2));
					if(a>b)
						counter2++;
					else if(b>a)
						counter1++;
					else
					{
						int same1 = counter1+1,same2=counter2+1;
						while(1)
						{
							if(same1 == first.size())
								break;
							float c = stof(first[same1][col1].substr(1,first[same1][col1].length()-2));
							if(c == a)
								same1++;
							else
								break;
						}
						while(1)
						{
							if(same2 == second.size())
								break;
							float c = stof(second[same2][col2].substr(1,second[same2][col2].length()-2));
							if(c == b)
								same2++;
							else
								break;
						}
						for(int m=counter1;m<same1;m++)
						{
							for(int n=counter2;n<same2;n++)
							{
								vector <string> temp_vec;
								temp_vec.reserve( first[m].size() + second[n].size() );
								temp_vec.insert( temp_vec.end(), first[m].begin(), first[m].end() );
								temp_vec.insert( temp_vec.end(), second[n].begin(), second[n].end() );
								to_return.push_back(temp_vec);
							}
						}
						counter1 = same1;
						counter2 = same2;
					}
				}
			}
			if(type == st)
			{
				int counter1 = 0;
				int counter2 = 0;
				while(1)
				{
					if(counter1>=first.size() || counter2>=second.size())
						break;
					string a = first[counter1][col1];
					string b = second[counter2][col2];
					if(a>b)
						counter2++;
					else if(b>a)
						counter1++;
					else
					{
						vector <string> temp_vec;
						temp_vec.reserve( first[counter1].size() + second[counter2].size() );
						temp_vec.insert( temp_vec.end(), first[counter1].begin(), first[counter1].end() );
						temp_vec.insert( temp_vec.end(), second[counter2].begin(), second[counter2].end() );
						to_return.push_back(temp_vec);
						counter1++;
						counter2++;
					}
				}
			}

			return to_return;
		}




		bool rec_compare(vector<string> rec1, vector<string> rec2, vector<string> cols, vector<string> cols_type, map<string,int> col_to_num)
		{
			for(j=0;j<cols.size();j++)
			{
				string st = "string";
				string fl = "float";
				string in = "int";
				if(cols_type[j]==fl || cols_type[j]==in)
				{
					if(stof(rec1[col_to_num[cols[j]]].substr(1,rec1[col_to_num[cols[j]]].length()-2)) > stof(rec2[col_to_num[cols[j]]].substr(1,rec2[col_to_num[cols[j]]].length()-2)))
						return true;
					else if(stof(rec1[col_to_num[cols[j]]].substr(1,rec1[col_to_num[cols[j]]].length()-2)) < stof(rec2[col_to_num[cols[j]]].substr(1,rec2[col_to_num[cols[j]]].length()-2)))
						return false;
				}
				if(cols_type[j]==st)
				{
					if(rec1[col_to_num[cols[j]]] > rec2[col_to_num[cols[j]]])
						return true;
					else if(rec1[col_to_num[cols[j]]] < rec2[col_to_num[cols[j]]])
						return false;
				}
			}
			return true;
		}



		void insert_into_cache(string st,int pos)
		{
			db[pos][0] = '\0';
			strcpy(db[pos],st.c_str());
			page_list[map_pages[pos]]->no_in_db = -1;
			db_compare_flag[pos] = 0;
		}






		vector <string> phase_merge(vector < vector<string> >  records, vector <string> cols, vector <string> cols_type, map<string,int> col_to_num,int ascflag)
		{
			int i,j,k,l;
			vector < vector<string> > phase1;
			vector <string> temp;
			if(records.size() == 0)
				return temp;
			for(i=0;i<records.size();i+=num_pages)
			{
				vector <string> temp;
				if(records.size()-i<num_pages)
				{
					for(j=i;j<records.size();j++)
					{
						string s = "";
						for(k=0;k<records[j].size();k++)
						{
							s += records[j][k];
							s+= ",";
						}
						insert_into_cache(s,j-i);
					}
					for(j=0;j<records.size()-i;j++)
					{
						int small = smallest_in_db(0,records.size()-i,cols,cols_type,col_to_num);
						db_compare_flag[small] = 1;
						string to_in(db[small]);
						temp.push_back(to_in);
					}
				}
				else
				{
					for(j=i;j<i+num_pages;j++)
					{
						string s ="";
						for(k=0;k<records[j].size();k++)
						{
							s += records[j][k];
							s += ",";
						}
						insert_into_cache(s,j-i);
					}
					for(j=0;j<num_pages;j++)
					{
						int small = smallest_in_db(0,num_pages,cols,cols_type,col_to_num);
						db_compare_flag[small] = 1;
						string to_in(db[small]);
						temp.push_back(to_in);
					}
				}
				phase1.push_back(temp);
			}
			vector < vector<string> > phase2 = phase1;
			while(phase2.size() != 1)
			{
				vector < vector<string> > main_temp;
				for(i=0;i<phase2.size();i+=num_pages)
				{
					vector <string> temp;
					int store_count[100000] = {0};
					if(phase2.size()-i<num_pages)
					{
						for(j=i;j<phase2.size();j++)
						{
							insert_into_cache(phase2[j][store_count[j-i]],j-i);
						}
						int breakflag =0;
						int counter = 0;
						while(1)
						{
							int small = smallest_in_db(0,phase2.size()-i,cols,cols_type,col_to_num);
							string to_in(db[small]);
							temp.push_back(to_in);
							store_count[small]++;
							if(phase2[i+small].size() == store_count[small])
							{
								breakflag++;
								db_compare_flag[small] = 1;
							}
							else
								insert_into_cache(phase2[i+small][store_count[small]],small);
							if(breakflag == phase2.size()-i)
								break;
						}

					}
					else
					{
						for(j=i;j<num_pages+i;j++)
						{
							insert_into_cache(phase2[j][store_count[j-i]],j-i);
						}
						int breakflag =0;
						while(1)
						{
							int small = smallest_in_db(0,num_pages,cols,cols_type,col_to_num);
							string to_in(db[small]);
							temp.push_back(to_in);
							store_count[small]++;
							if(phase2[i+small].size() == store_count[small])
							{
								breakflag++;
								db_compare_flag[small] = 1;
							}
							else
								insert_into_cache(phase2[i+small][store_count[small]],small);
							if(breakflag == num_pages)
								break;
						}
					}
					main_temp.push_back(temp);
				}
				phase2 = main_temp;
			}
			vector <string> ans = phase2[0];
			if(ascflag==1)
				reverse(ans.begin(),ans.end());
			return ans;


		}




		int smallest_in_db(int start,int end,vector <string> cols, vector <string> cols_type, map<string,int> col_to_num)
		{
			int s = start;
			while(db_compare_flag[s]!=0)
				s++;
			for(i=0;i<end;i++)
			{
				if(db_compare_flag[i]==0)
				{
					if(db_compare(i,s,cols,cols_type,col_to_num))
					{
						s = i;
					}
				}
			}
			return s;
		}



		bool db_compare(int n, int m,vector <string> cols, vector <string> cols_type, map<string,int> col_to_num)
		{
			vector <string> rec1;
			vector <string> rec2;
			string str1(db[n]);
			string str2(db[m]);
			Tokenizer r1;
			r1.set(str1);
			r1.setDelimiter(",");
			string t1;
			while((t1 = r1.next())!="")
				rec1.push_back(t1);
			Tokenizer r2;
			r2.set(str2);
			r2.setDelimiter(",");
			string t2;
			while((t2 = r2.next())!="")
				rec2.push_back(t2);
			return rec_compare(rec1,rec2,cols,cols_type,col_to_num);
		}


		int v(string table_name, string col_name)
		{
			return unique_counts[table_name + " " + col_name];
		}
















		void queryType(string query)
		{
			string test_string = query;
			Tokenizer tok;
			string token;
			tok.setDelimiter(" ");
			tok.set(test_string);
			string tem = tok.next();
			map <string,DBSystem> dbmap;
			transform(tem.begin(), tem.end(), tem.begin(),(int (*)(int))tolower);
			if(tem == "create")
				createCommand(query);
			if(tem == "select")
				selectCommand(query);
			if(query[0] == 'v')
			{
				string temp = query.substr(2,query.length()-3);
				Tokenizer tk;
				tk.set(temp);
				tk.setDelimiter(",");
				string t1 = tk.next();
				string t2 = tk.next();
				string s = t1 + " " + t2;
				cout << unique_counts[s] << endl;
			}
		}



};

int main()
{
	DBSystem dbs;
	dbs.readConfig("./");
	dbs.populatePageInfo();
	string command;/*
			  while(1)
			  {
			  getline(cin,command);
			  if(command.find("insertRecord") < command.length())
			  {
			  Tokenizer tk;
			  tk.set(command);
			  tk.setDelimiter("(");
			  tk.next();
			  string t = tk.next();
			  tk.set(t);
			  tk.setDelimiter(")");
			  string rec = tk.next();
			  tk.set(rec);
			  tk.setDelimiter(",");
			  string tab = tk.next();
			  vector <string> temp;
			  string tp;
			  string r;
			  while((tp = tk.next())!="")
			  {
			  r += tp;
			  r += ",";
			  }
			  dbs.insertRecord(tab.substr(1,tab.length()-2),r.substr(1,r.length()-3));
			  }
			  else if(command.find("getRecord") < command.length())
			  {
			  Tokenizer tk;
			  tk.set(command);
			  tk.setDelimiter("(");
			  tk.next();
			  string t = tk.next();
			  tk.set(t);
			  tk.setDelimiter(")");
			  string rec = tk.next();
			  tk.set(rec);
			  tk.setDelimiter(",");
			  string tab = tk.next();
			  string rn = tk.next();
			  int rec_num = stoi(rn);
			  dbs.getRecord(tab.substr(1,tab.length()-2),rec_num);
			  }
			  else if(command.find("flushPages") < command.length())
			  {
			  dbs.flushPages();
			  }

			  if(command == "quit" || command == "q")
			  break;
			  else
			  dbs.queryType(command);
			  }*/
//	dbs.queryType("Select * from countries");
//	dbs.queryType("Select TYPE,NAME from airports");
//	dbs.queryType("Select ID from airports where TYPE=small_airport");
//	dbs.queryType("Select ID from airports where TYPELIKESMALL_Airport");
//	dbs.queryType("Select ID,NAME,CODE,CONTINENT from countries where CONTINENT=OC or NAME=India");
//	dbs.queryType("Select ID,NAME from airports orderby NAME(ASC)");
	dbs.queryType("Select NAME,COUNTRY from airports where LATITUDE<=42 orderby NAME(ASC)");
	return 0;
}
