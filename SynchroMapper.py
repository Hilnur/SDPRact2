'''
Created on 8 may. 2019

@author: David Fernandez Marquez
@author: Maite Bernaus Gimeno

'''
import json
import pywren_ibm_cloud as pywren
import pika, random

def myServerFunction(exchangeName, nodes):
    url= 'amqp://acsihjnn:Nzc7ufwFU7IDp4if5n6J6o3e4hgwoUOa@caterpillar.rmq.cloudamqp.com/acsihjnn'
    params = pika.URLParameters(url)
    connection = pika.BlockingConnection(params)
    channel = connection.channel() # start a channel
    channel.queue_declare(queue='petitions')
    channel.queue_purge('petitions')
    channel.exchange_declare(exchange=exchangeName, exchange_type='fanout')
    
    numnodes=int(nodes)
    myexchange=exchangeName
    messagecounter=0
    identifiers=[]
    print('Master setup ready. Server set for %i nodes. Nodes will communicate through the exchange %s' % (numnodes, myexchange))
    
    #Function for the basic_consume configuration. 
    def receivePetition(ch, method, properties, body):
        nonlocal identifiers
        nonlocal messagecounter
        nonlocal numnodes
        
        idm=body.decode()
        print('Master node receiving identifier %s' % (idm))
        identifiers.append(idm)
        print(identifiers)
        messagecounter=messagecounter+1
        if messagecounter>= numnodes:
            ch.stop_consuming()
    
    channel.basic_consume(receivePetition, queue='petitions', no_ack=True)
    channel.start_consuming()
    channel.queue_purge('petitions')
    
    print('Master has received all petitions')
    print("Master node message count: %i" % (messagecounter))
    messagecounter=0
    #shuffle list
    random.shuffle(identifiers)
    print(identifiers)
    
    def managePermissions(ch, method, properties, body):
        nonlocal numnodes
        nonlocal myexchange
        nonlocal identifiers
        nonlocal messagecounter
        msg=json.loads(body)
        print('Server receiving permission message:')
        print(msg)
        if 'done' in msg:
            messagecounter=messagecounter+1
            if messagecounter<numnodes:
                ch.basic_publish(exchange='', routing_key=str(identifiers[messagecounter]), body=json.dumps({'permission':1}))
            else:
                print('All messages treated. Sending Finish order')
                ch.basic_publish(exchange=myexchange, routing_key='', body=json.dumps({'finished':1}))
                ch.stop_consuming()
        elif 'error' in msg: 
            print('Something has gone wrong here')
            
    
    channel.basic_publish(exchange='', routing_key=str(identifiers[messagecounter]), body=json.dumps({'permission':1}))
    channel.basic_consume(managePermissions, queue='petitions', no_ack=True)
    channel.start_consuming()
    channel.queue_purge('petitions')
    channel.close()
    return {'nodeWriteOrder':identifiers}
    
def my_map_function(idm, exchangeName):
    
    results=[]
    url = 'amqp://acsihjnn:Nzc7ufwFU7IDp4if5n6J6o3e4hgwoUOa@caterpillar.rmq.cloudamqp.com/acsihjnn'
    params = pika.URLParameters(url)
    connection = pika.BlockingConnection(params)
    channel = connection.channel() # start a channel
    
    identif=idm
    myvalue=random.randint(0, 1000)
    myqueue=str(idm)
    myexchange=exchangeName
    
    channel.queue_declare(queue=myqueue)
    channel.queue_bind(queue=myqueue, exchange=myexchange)
    channel.queue_purge(queue=myqueue) #In case of other processes having previously used a queue with the same name and not emptying it, we'll purge the queue before starting
    
    print('I am node %s. I have declared queue %s. I have bound it to exchange %s. My value is %s' % (identif, myqueue, myexchange, myvalue))
    
    #Send first petition
    channel.publish(exchange='', routing_key='petitions', body=myqueue)
    print('Node %s has sent write petition' % (identif))
    
    def treatMapperMessage(ch, method, properties, body):
        nonlocal results
        nonlocal myexchange
        nonlocal myqueue
        msg=json.loads(body)
        print(msg)
        if ('result' in msg):
            results.append(msg.get('result'))
        elif ('permission' in msg):
            message=json.dumps({'result':myvalue})
            print('Node %s received permission to write. Sending to all' % (myqueue))
            ch.publish(exchange=myexchange, routing_key='', body=message)
            ch.publish(exchange='', routing_key='petitions', body=json.dumps({'done':myqueue}))
        elif ('finished' in msg):
            print('Node %s received a finish' % (myqueue))
            channel.stop_consuming()
            channel.queue_delete(queue=myqueue) #The queue should be empty. If it isn't, an error has happened, so we leave the queue so the admin can check it
            channel.close()
        
    channel.basic_consume(treatMapperMessage, queue=myqueue, no_ack=True)
    channel.start_consuming()
    print('Node %s finishing and returning' % (myqueue))
    return {str(identif):results}


def ExecuteMap(numnodes, exchangeName):
    """   
    Performs the split mapping through pywren and prints the results of the process through standard output.
    Params:
        numnodes: the number of map nodes you wish to spawn
        exchangeName: the exchange you want the nodes to use for communication. If the exchange exists, it must 
        be of fanout type. If it doesn't, the function will create it.
    
    Returns: Nothing
    
    Output:
        The output format is a list of dictionaries, where list[0] is the output from the master node (which
        stores the order in which nodes have written), and then the ordered output of each node as a dictionary
    {node_identifier:map_results}
    """
    
    pw = pywren.ibm_cf_executor(rabbitmq_monitor=True)
    #start server
    args={'exchangeName':exchangeName, 'nodes':numnodes}
    pw.call_async(myServerFunction, args)
    #start nodes
    iterdata=[]
    params={'idm':0, 'exchangeName':exchangeName}
    for i in range(numnodes):
        params['idm']=i
        iterdata.append(params.copy())
    pw.map(my_map_function, iterdata)
    result = pw.get_result()
    print(result)
    
#ExecuteMap(19, 'testExchange')
