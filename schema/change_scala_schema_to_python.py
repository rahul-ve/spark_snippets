import re
from collections import deque

##

def findMatchingBrace(text2, ind, d2):
    # read the top most item in the deque
    top = d2.pop()
    d2.append(top)
    
    paran  = re.compile(r'[)(]')
    found_paran = re.search(paran, text2[ind:])
    
    #recursively find the closing paranthesis
    if found_paran:
        if found_paran.group() == ')' and len(d2) == 1 and top[0] == '(':
            return found_paran.span()[0]+ind
        elif found_paran.group() == '(' and top[0] == '(':
            d2.append((found_paran.group(), (found_paran.span()[0]+ind)))
            return findMatchingBrace(text2, found_paran.span()[1]+ind, d2)
        elif found_paran.group() == ')' and top[0] == '(' and len(d2) > 1:
            d2.pop()
            return findMatchingBrace(text2, found_paran.span()[1]+ind, d2)
        else:
            return ind
    else:
        return ind

##

def replaceList(text1):
    listAnchor = re.compile(r'List[(]')
    anchor = re.search(listAnchor, text1)

    if anchor:
        #clear deque before matching the closing paranthesis for "List("
        d1.clear()
        # add the opening paranthesis index to deque
        d1.append(('(', anchor.span()[1]-1)) 
        matchedBrace = findMatchingBrace(text1, anchor.span()[1], d1) 
        
        # first replace the closing paranthesis before replacing the opening as the opening one is "List(" NOT just "("
        fixedClose = "".join((text1[:matchedBrace], ']', text1[matchedBrace+1:]))
        fixedOpen = "".join((fixedClose[:anchor.span()[0]], '[' , fixedClose[anchor.span()[1]:]))
        
        #recursively try to fix the next "List("
        return replaceList(fixedOpen)

    else:
        return text1

##

def convertSchema(text_in):
  """
      Usage: python_schema_structure = convertSchema(scalaSchemaString)
  """
  
  global d1 
  d1 = deque()
  
  #first replace all "List()" with "[]"
  step1 = replaceList(text_in)
  
  # replace all "true" with True
  step2 = re.sub(r'(?<=,)(true)(?=\))', 'True', step1)
  
  # replace all "false" with False
  step3 = re.sub(r'(?<=,)(false)(?=\))', 'False', step2)
  
  # add parantheses to types
  step4 = re.sub(r'(?<=,|\()([a-zA-Z]+Type)(?=,(True|False))', r'\g<1>()', step3)
  
  # quote the field names
  step5 = re.sub(r'(?<=StructField\()((\w)+)(?=,)', r'"\g<1>"', step4)
  
  return step5
  

##

"""

## ONE-LINER
print(re.sub(r'(StructField)(?=\()', r'\n\g<1>', convertSchema(str(someDF.schema))))

##########################################################


## SAVE DATAFRAME SCHEMA AS JSON
scala_schema_json = someDF.schema.json()

## SAVE SCHEMA TO DISK
with open(EXAMPLE_SCHEMA_JSON_PATH, 'w') as f1:
    json.dump(scala_schema_json, f1)


## READ SCHEMA FROM DISK
with open(EXAMPLE_SCHEMA_JSON_PATH, 'r') as f:
    example_schema = json.load(f)
exampleSchemaScala = StructType.fromJson(json.loads(example_schema))
exampleSchemaScala  # struct object!   # same as "someDF.schema"


## CONVERT TO PYTHON FORMAT
exampleSchemaPython = convertSchema(str(exampleSchemaScala))

## PRETTY PRINT
perLine = re.sub(r'(StructField)(?=\()', r'\n\g<1>', exampleSchemaPython)
print(perLine)

"""
