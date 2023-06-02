import csv
import json

#-------------------2020510091 and 2021510069---------------------#
#-------------Evrim Gizem İşci and Eylül Pınar Yetişkin-----------#
 
# parse condition
def parse_condition(condition):
    condition = condition.strip()
    operators = ['<=', '>=', '!=','!<', '!>' ,'<', '>', '=']
    #if condition has one of the operator, split by this operator
    for operator in operators:
        if operator in condition:
            condition_column, condition_value = condition.split(operator)
            condition_column = condition_column.strip()
            condition_operator = operator
            condition_value = condition_value.strip().replace("'", "")
            return condition_column, condition_operator, condition_value
        #parse condition if it has and , or
def parse_and_or(condition):
    #remove spaces
    condition = condition.strip().upper()
    operators = ['<=', '>=', '!=','!<', '!>' ,'<', '>', '=']
    #split by condition
    if ' AND ' in condition:
        logical_operator = 'AND'
        condition_parts = condition.split(' AND ')
   
    elif ' OR ' in condition:
        logical_operator = 'OR'
        condition_parts = condition.split(' OR ')
    
    
    results = []
    for part in condition_parts:
       # Find the operator used in the condition
       operator = None
       for op in operators:
           if op in part:
               operator = op
               break
           #split by operator and remove spaces. also add the results
       if operator:
           condition_column, condition_value = part.split(operator)
           condition_column = condition_column.strip()
           condition_operator = operator
           condition_value = condition_value.strip().replace("'", "")
           results.append((condition_column, condition_operator, condition_value))
    return logical_operator, results
           
      #evaluate condition and return it
def evaluate_condition(value, operator, condition_value):
    
    if operator == '=':
        return value.lower() == condition_value.lower()
    elif operator == '!=':
        return value.lower() != condition_value.lower()
    elif operator == '<':
        return int(value) < int(condition_value)
    elif operator == '>':
        return int(value) > int(condition_value)
    elif operator == '<=':
        return int(value) <= int(condition_value)
    elif operator == '>=':
        return int(value) >= int(condition_value)  
    elif operator == '!<':
        return int(value) >= int(condition_value)
    elif operator == '!>':
        return int(value) <= int(condition_value)
    

    
def execute_select(data, columns, condition, order):
    results = []

    # Parse condition
    condition_column = None
    condition_operator = None
    condition_value = None
    logical_operator = None
    if 'AND' in condition.upper() or 'OR' in condition.upper():
        logical_operator, condition_parts = parse_and_or(condition)
        for record in data:
            if logical_operator.upper() == 'AND':
                # Check if all conditions are satisfied
                all_conditions_satisfied = True
                for column, operator, value in condition_parts:
                    #make case insensitive
                    column_lower = column.lower()
                    value_lower = value.lower()
                    if not evaluate_condition(record.get(column_lower), operator, value_lower.lower()):
                        all_conditions_satisfied = False
                  #if conditions satisfied, add results list      
                if all_conditions_satisfied:
                    result = {column: record[column] for column in columns}  
                    results.append(result)
            elif logical_operator.upper() == 'OR':
                # Check if any condition is satisfied
               any_condition_satisfied = False
               for column, operator, value in condition_parts:
                   # Make case insensitive
                   column_lower = column.lower()
                   value_lower = value.lower()
                   if evaluate_condition(record.get(column_lower), operator, value_lower.lower()):
                       any_condition_satisfied = True
                       break  # Exit loop if any condition is satisfied
               # If any condition satisfied, add to results list
               if any_condition_satisfied:
                   result = {column: record[column] for column in columns}  
                   results.append(result)  
     #if condition has symbol like <,>, etc.               
    else:
        condition_column, condition_operator, condition_value = parse_condition(condition)
        for record in data:
            if evaluate_condition(record.get(condition_column.lower()), condition_operator, condition_value.lower()):
                result = {column: record[column] for column in columns}
                results.append(result)
    
    sort_column = list(columns)[0]                  
    #sort result by order info
    if order:
        order = order.upper()
        if order == 'ASC' and sort_column!='ALL':
            results.sort(key=lambda x: x.get(sort_column, 0))
        elif order == 'DSC'and sort_column!='ALL':
            results.sort(key=lambda x: x.get(sort_column, 0), reverse=True)
        elif order == 'ASC'and sort_column=='ALL':
            results.sort(key=lambda x: int(x.get('id', 0)))
        elif order == 'DSC'and sort_column=='ALL':
            results.sort(key=lambda x: int(x.get('id', 0)), reverse=True)
        
    return results
# Execute INSERT query
def execute_insert(data, values):
    data = json.loads(data)
    #create record dictionary     
    record = {'id': values[0], 'name': values[1], 'lastname': values[2], 'email': values[3], 'grade': values[4]}
    data.append(record)
    data.sort(key=lambda x: int(x['id']))
 
    return data
    

# Execute DELETE query
def execute_delete(data, condition):
    
    results = []
    # parse condition
    condition_column = None
    condition_operator = None
    condition_value = None
    logical_operator = None
    if 'AND' in condition.upper() or 'OR' in condition.upper():
        logical_operator, condition_parts = parse_and_or(condition)
        for record in data:
            if logical_operator.upper() == 'AND':
                # Check if all conditions are satisfied
                all_conditions_satisfied = True
                for column, operator, value in condition_parts:
                    #make case insensitive
                    column_lower = column.lower()
                    value_lower = value.lower()
                    if not evaluate_condition(record.get(column_lower), operator, value_lower):
                        all_conditions_satisfied = False
                    #if condition is not satisfied, add the result    
                if not all_conditions_satisfied:
                    result = record.copy()  # Create a copy of the record
                    results.append(result)
            elif logical_operator.upper() == 'OR':
                # if condition is not satisfied, add the result.
                # Check if any condition is satisfied
               any_condition_satisfied = False
               for column, operator, value in condition_parts:
                   # Make case insensitive
                   column_lower = column.lower()
                   value_lower = value.lower()
                   if evaluate_condition(record.get(column_lower), operator, value_lower.lower()):
                       any_condition_satisfied = True
                       break  # Exit loop if any condition is satisfied
               # If any condition not satisfied, add to results list
               if not any_condition_satisfied:
                   result = record.copy()  # Create a copy of the record
                   results.append(result)  
                    
    else:
        condition_column, condition_operator, condition_value = parse_condition(condition)
        for record in data:
            #if condition is not satisfied then add the result list
            if not evaluate_condition(record.get(condition_column.lower()), condition_operator, condition_value.lower()):
                result = record.copy()
                results.append(result)
    return results            
# Parse and execute the query
def execute_query(data, query):
    query_parts = query.split()
    query_type = query_parts[0].upper()
    #select condition
    if query_type == 'SELECT':
        #find columns,condition and order part in query
        columns = query_parts[1:query_parts.index('FROM')]
        condition_start_index = query_parts.index('WHERE') + 1
        condition_end_index = query_parts.index('ORDER') if 'ORDER' in query_parts else len(query_parts)
        condition = ' '.join(query_parts[condition_start_index:condition_end_index])
        order = query_parts[-1] if 'ORDER' in query_parts else None
        data = json.loads(data)
        #if columns size is 1 and columns is ALL
        if len(columns) == 1 and columns[0] == 'ALL':
            columns = data[0].keys()
        else:
            #removes leading and trailing spaces of each word
            columns = [col.strip() for col in columns]
            #creates a new list by separating the columns with commas
            columns = [column for col in columns for column in col.split(',')]

        results = execute_select(data, columns, condition, order)
        return results
    #insert condition
    elif query_type == 'INSERT':
        #get values ands split by comma
        values_start_index = query.index('VALUES(') + len('VALUES(')
        values_end_index = query.index(')')
        values = query[values_start_index:values_end_index].split(',')
        if(len(values) != count_delimited_values("students.csv")) :
            print("You entered incomplete values.")
            
        else: return execute_insert(data, values)
        
        #delete condition
    elif query_type == 'DELETE':
        #get condition part
        condition_start_index = query.index('WHERE') + len('WHERE') + 1
        condition = query[condition_start_index:]
        condition = condition.strip()
        #convert json to python format
        data = json.loads(data)
        results = execute_delete(data, condition)
        return results

    else:
        print("Error: Invalid query type.")
        
#load csv file
def load_csv(file_path):
    with open(file_path, 'r') as file:
        #read line by line and split by ;
        reader = csv.DictReader(file, delimiter=';')
        data = list(reader)
        #sort data by increasing order
    sorted_data = sorted(data, key=lambda x: int(x['id']))
    json_data = json.dumps(sorted_data)
    #print(json_data)
    return json_data

#save json file
def save_json(file_name, data):
    with open(file_name, 'w') as file:
        json.dump(data, file, indent=4)
        
#find column count
def count_delimited_values(file_path, delimiter=';'):
    with open(file_path, 'r') as file:
        line = file.readline().strip()  #read first line and remove spaces
        value_count = line.count(delimiter) + 1 #find delimiter count and add 1
    return value_count

# Main 
data = load_csv("students.csv")

while True:
    
    query = input("Enter query (or 'exit'): ").strip() #take input and remove spaces from this input
    if query.lower() == 'exit':
        break
        
    try:
        select_operation = 'SELECT' in query.upper()
        #execute query
        results = execute_query(data, query)
        if results is not None:
            print(json.dumps(results, indent=4))
            if not select_operation:
                #save file
                json_file_name = 'query_results.json'
                save_json(json_file_name, results)
                print(f"Query results saved as {json_file_name}")
                with open(json_file_name, 'r') as file:
                    data = json.load(file)
                data = json.dumps(data)
    except Exception as e:
        print(f"Error executing query: {e}")














