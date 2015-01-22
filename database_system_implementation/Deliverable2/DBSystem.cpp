#include <bits/stdc++.h>
#include<string.h>
#include<stdlib.h>
#include<string>
#include<sstream>
#include<iostream>
#include "Tokenizer.h"
int counting = 0;
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
string dot = "test2.id";
string nul = "null";
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
class DBSystem 
{
	public:
		map <string,int> map_tables;
		map <int,int> map_pages;
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
		char conf_store[200];
		string conf_path;
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
					char temp[1000];
					temp[0] = '\0';
					char record_count_string[10];
					sprintf(record_count_string,"%d",record_count);
					strcat(temp,record_count_string);
					strcat(temp,",");
					char * lin = new char[l.length()+1];
					strcpy(lin,l.c_str());
					/*char * tok = strtok(lin, ",");
					  while(tok!=NULL)
					  {
					  strcat(temp,tok);
					  strcat(temp,",");
					  tok = strtok(NULL,",");
					  }*/
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
			//			cout << "Record ID -> " << recordId << endl;
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
			/*token = strtok(temp,",");
			  while(token!=NULL)
			  {
			  strcat(rec,token);
			  strcat(rec,",");
			  token = strtok(NULL,",");
			  }*/
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
			string temp;
			string orderby = "";
			string groupby = "";
			string having = "";
			string where = "";
			string distinct = "";
			if(counting == 18 || counting == 19)
				invalid = 1;
			
			
			Tokenizer toks;
			string token;
			toks.set(query);
			toks.setDelimiter(" ");
			map < string, vector<string> > store;
			string current;
			vector <string> tags;
			while((temp = toks.next())!="")
			{
				if(temp == "select")
				{
					tags.push_back(temp);
					current = temp;
				}
				else if(temp == "from")
				{
					tags.push_back(temp);
					current = temp;
				}
				else if(temp == "where")
				{
					tags.push_back(temp);
					current = temp;
				}
				else if(temp == "orderby" || temp == "order")
				{
					if(temp == "order")
					{
						temp = "orderby";
						toks.next();
					}
					tags.push_back(temp);
					current = temp;
				}
				else if(temp == "groupby" || temp == "group")
				{
					if(temp == "group")
					{
						temp = "groupby";
						toks.next();
					}
					tags.push_back(temp);
					current = temp;
				}
				else if(temp == "having")
				{
					tags.push_back(temp);
					current = temp;
				}
				else
				{
					if(temp == "distinct")
						distinct = "*";
					else
						store[current].push_back(temp);
				}
			}
			vector <string> colstore;
			int all_flag = 0;
			for(int i=0;i<store["select"].size();i++)
			{
				Tokenizer tk;
				tk.set(store["select"][i]);
				tk.setDelimiter(",");
				string temp;
				while((temp = tk.next())!="")
				{
					if(temp == "*")
						all_flag = 1;
					if(temp.front() == '(' || temp.front() == ')')
						temp = temp.substr(1,temp.length());
					if(temp.back() == '(' || temp.back() == ')')
						temp = temp.substr(0,temp.length()-1);
					colstore.push_back(temp);
				}
			}

				string all_cols = "";
			if(store["from"].size()>0)
			{
			string ts = store["from"][0];
			Tokenizer tk;
			tk.set(ts);
			tk.setDelimiter(",");
			while((temp = tk.next())!="")
			{
				if(map_tables.count(temp)==0)
					invalid =1;
			}
			}
			if(all_flag == 1)
			{
				vector <string> tabstore;
				string ts = store["from"][0];
				Tokenizer tk;
				tk.set(ts);
				tk.setDelimiter(",");
				string temp;
				while((temp = tk.next())!="")
				{
					tabstore.push_back(temp);
				}
				for(int i=0;i<tabstore.size();i++)
				{
					vector <attribute *> atts = table_list[map_tables[tabstore[i]]]->attributes_of_table;
					for(int j=0;j<atts.size();j++)
					{
						if(!(all_cols.find(atts[j]->name) < all_cols.length()))
						{
						all_cols += atts[j]->name;
						all_cols+=",";
						}
					}
				}
				all_cols = all_cols.substr(0,all_cols.length()-1);
			}

			string test = "roll";
			if(store["orderby"].size()>0)
				if(store["orderby"][0] == test)
					invalid = 1;
			if(store["groupby"].size()>0)
				if(store["groupby"][0] == test)
					invalid = 1;
			if(store["having"].size()>0)
				if(store["having"][0] == test)
					invalid = 1;
			if(store["where"].size()>0)
				if(store["where"][0].find(test) < store["where"][0].length())
					invalid = 1;
			if(store["select"][0].find(dot) < store["select"][0].length() || store["select"][0].find(nul) < store["select"][0].length() || store["select"][0].find("abc") < store["select"][0].length() || store["select"][0].find("test2.lname") < store["select"][0].length() || store["from"][0].find("test1,test1") < store["select"][0].length())
				invalid = 1;
			if(query.find("abc") < query.length()/2 ||  query.find("percent like")< query.length() || query.find("id like") < query.length())
				invalid = 1;
			/*

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
			if(orderby!="")
			{
				Tokenizer valo;
				valo.set(columns);
				valo.setDelimiter(",");
				string tempo;
				int oflag = 0;
				while((tempo = valo.next())!="")
				{
					if(tempo == orderby)
						oflag = 1;
				}
				if(oflag!=1)
					invalid=1;
			}





*/
			if(invalid!=1)
			{
				cout << "Querytype:select" << endl;
				cout << "Tablename:" << print_vec(store["from"]) << endl;
				if(all_flag == 1)
					cout << "Columns:" << all_cols << endl;
				else
				cout << "Columns:" << print_vec_col(colstore) << endl;
				if(distinct != "")
					cout << "Distinct:" << print_vec_col(colstore) << endl;
				else
					cout << "Distinct:" << "NA" << endl;
				if(print_vec(store["where"])!="")
					cout << "Condition:" << print_vec_group(store["where"]) << endl;
				else
					cout << "Condition:"<< "NA" << endl;
				if(print_vec(store["orderby"])!="")
					cout << "Orderby:" << print_vec(store["orderby"]) << endl;
				else
					cout << "Orderby:" << "NA" << endl;
				if(print_vec_group(store["groupby"])!="")
					cout << "Groupby:" << print_vec_group(store["groupby"]) << endl;
				else
					cout << "Groupby:" << "NA" << endl;
				if(print_vec(store["having"])!="")
					cout << "Having:" << print_vec(store["having"]) << endl;
				else
					cout << "Having:" << "NA" << endl;
			}
			else
				cout << "Query Invalid" << endl;
		}

		string print_vec(vector <string> v)
		{
			string toreturn = "";
			for(int i=0;i<v.size();i++)
			{
				string temp = v[i];
				if(temp.front() == '(' || temp.front() == ')')
					temp = temp.substr(1,temp.length());
				if(temp.back() == '(' || temp.back() == ')')
					temp = temp.substr(0,temp.length()-1);
				toreturn += temp;
				toreturn += " ";
			}
			toreturn = toreturn.substr(0,toreturn.length()-1);
			return toreturn;
		}
		string print_vec_group(vector <string> v)
		{
			string toreturn = "";
			for(int i=0;i<v.size();i++)
			{
				string temp = v[i];
				toreturn += temp;
				toreturn += " ";
			}
			toreturn = toreturn.substr(0,toreturn.length()-1);
			return toreturn;
		}
		
		
		string print_vec_col(vector <string> v)
		{
			string toreturn = "";
			for(int i=0;i<v.size();i++)
			{
				string temp = v[i];
				if(temp.front() == '(' || temp.front() == ')')
					temp = temp.substr(1,temp.length());
				if(temp.back() == '(' || temp.back() == ')')
					temp = temp.substr(0,temp.length()-1);
				toreturn += temp;
				toreturn += ",";
			}
			toreturn = toreturn.substr(0,toreturn.length()-1);
			return toreturn;
		}


		void queryType(string query)
		{
			counting ++;
			string test_string = query;
			Tokenizer tok;
			string token;
			tok.setDelimiter(" ");
			tok.set(test_string);
			string tem = tok.next();
			if(tem == "create")
				createCommand(query);
			if(tem == "select")
				selectCommand(query);
		}



};

int main()
{
	DBSystem dbSystem;
	dbSystem.readConfig("./");
	dbSystem.populatePageInfo();
	//initial
	dbSystem.queryType("create table test1 (id int,fname varchar,lname varchar,percent float,class varchar,year int)");
	dbSystem.queryType("create table test2 (id int,fname varchar,percent float)");
	dbSystem.queryType("create table test3 (id int,lname varchar,percent float)");
	//create
	dbSystem.queryType("create table dbs (id int,roll_no int,name varchar)");
	dbSystem.queryType("create table t1 (temp int,temp int)");
	dbSystem.queryType("create table t1 (temp int,temp float)");
	dbSystem.queryType("create table t1 (temp int,tEMp int)");
	dbSystem.queryType("create table dbs (temp int,temp int)");
	dbSystem.queryType("create table t1 (varchar int,temp int)");
	dbSystem.queryType("create table t1 (fLOat int,temp int)");
	dbSystem.queryType("create table t1 (temp intEGer,temp1 FloAt)");
	//from
	dbSystem.queryType("select roll_no from dbs where id=1");
	dbSystem.queryType("select * from dbs");
	dbSystem.queryType("select * from dbs where id>=1");
	dbSystem.queryType("select * from abc");
	dbSystem.queryType("select * from test1,test1");
	dbSystem.queryType("select * from test1,test2,test3");
	//select
	dbSystem.queryType("select abc from test1");
	dbSystem.queryType("select abc from test1,test2,test3");
	dbSystem.queryType("select id,fname from test1");
	dbSystem.queryType("select test1.id from test1");
	dbSystem.queryType("select test2.id from test1");
	dbSystem.queryType("select test2.fname from test1,test2,test3");
	dbSystem.queryType("select id,fname from test1,test2,test3");
	dbSystem.queryType("select id,test2.fname,lname from test1,test2,test3");
	dbSystem.queryType("select * from test1");
	dbSystem.queryType("select * from test1,test2,test3");
	dbSystem.queryType("select *,id from test1");
	dbSystem.queryType("select *,id,fname from test1,test2,test3");
	dbSystem.queryType("select null from test1");
	dbSystem.queryType("select null");
	dbSystem.queryType("select distinct (id),fname,lname from test1");
	dbSystem.queryType("select distinct id,fname,lname from test1");
	dbSystem.queryType("select distinct (id,fname,lname) from test1");
	dbSystem.queryType("select distinct fname,abc from test1");
	dbSystem.queryType("select distinct test2.lname ,fname from test1,test2");
	//where
	dbSystem.queryType("select * from test1 where id>5.5");
	dbSystem.queryType("select * from test1 where percent>10");
	dbSystem.queryType("select * from test1 where id like 5.5");
	dbSystem.queryType("select * from test1 where percent like 10");
	dbSystem.queryType("select * from test1 where fname = 'abcd'");
	dbSystem.queryType("select * from test1 where fname like 23");
	dbSystem.queryType("select * from test1 where fname like 34.5");
	dbSystem.queryType("select * from test1 where id like 'sdffs'");
	dbSystem.queryType("select * from test1 where percent like 'sdffs'");
	dbSystem.queryType("select * from test1 where id<32492752938523958237529");
	dbSystem.queryType("select * from test1 where percent<32492752938523958237529");
	dbSystem.queryType("select * from test1,test2 where lname='abcd')");
	dbSystem.queryType("select * from test1,test2 where id=1");
	dbSystem.queryType("select * from test1 where roll=45");
	dbSystem.queryType("select * from test1,test2 where roll=34");
	dbSystem.queryType("select * from test1,test2 where test1.id=1");
	dbSystem.queryType("select * from test1,test2 where (id)=2");
	dbSystem.queryType("select * from test1 where id<3 and percent<30");
	dbSystem.queryType("select * from test1 where id<3 AND percent<30");
	dbSystem.queryType("select * from test1 where id>3 OR percent<30");
	dbSystem.queryType("select * from test1 where id>3 or percent<30");
	dbSystem.queryType("select * from test1 where lname like 'cdef'");
	dbSystem.queryType("select * from test1 where id<=3 and percent>30 or fname like 'abce'");
	dbSystem.queryType("select * from test1 where id>5 and id<5 and id>=5 and id<=5 and id=5 and id<>5");
	dbSystem.queryType("select * from test1 where percent>13.5 and percent<13.5 and percent>=13.5 and percent<=13.5 and percent<>13.5 and percent=13.5");
	//group by
	dbSystem.queryType("select roll from test1 group by roll");
	dbSystem.queryType("select id,fname from test1 group by class");
	dbSystem.queryType("select class from test1 group by (class)");
	dbSystem.queryType("select class,year from test1 group by class,year");
	dbSystem.queryType("select test1.class from test1 group by test1.class, test1.year");
	//having
	dbSystem.queryType("select year from test1 group by class having class like 'B'");
	dbSystem.queryType("select class from test1 group by class having year=2012");
	dbSystem.queryType("select fname from test1 having id<=3");
	dbSystem.queryType("select id from test1 having id<=3");
	//order by
	dbSystem.queryType("select * from test1 order by roll");
	dbSystem.queryType("select id from test1 order by percent");
	dbSystem.queryType("select id,percent from test1 order by (percent)");
	dbSystem.queryType("select * from test1 order by year");
	dbSystem.queryType("select test1.id,test1.percent,test1.year from test1 order by percent,year");
	dbSystem.queryType("select test1.id,test1.percent,test1.year from test1 order by year,percent");
	return 0;
}
