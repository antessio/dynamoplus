import React, {useState} from 'react'

import './Indexes.css'
import Loading from '../../components/loading/Loading'
import {List,Button } from 'antd'
import {useGetIndexes} from '../../hooks/indexes'
import {useCreateIndex} from '../../hooks/indexes'
import Index from '../../components/indexes/Index'
import CreateIndexForm from './create/CreateIndexForm'
const Indexes = (props)=>{
    const collection = props.match.params.collection
    const [indexes,isLoading] = useGetIndexes([],collection);
    const [indexCreated,createIndex, isLoadingCreateIndex]=useCreateIndex()
    const [showModal,setShowModal]=useState(false)
    if (isLoading && !indexes) {
        return <Loading />
      }
    return(<div>
        <h2>Indexes</h2>
        <p>{collection}</p>
        <Button type="primary" icon="plus"
          onClick={()=>{setShowModal(true)}}>
          Create
        </Button>
        {showModal && 
          <CreateIndexForm 
          show={showModal} 
          onCancel={()=>{setShowModal(false)}} 
          onSubmit={(values)=>{
            let indexName = values.fields.join("__")
            if(values.orderBy){
              indexName=indexName+"__ORDER_BY__"+values.orderBy
            }
            createIndex({
              collection:{
                name: collection
              },
              name: indexName
            })
            setShowModal(false)
          }}
          onError={(e)=>console.error(e)}
          />}
        {!isLoading && indexes &&  <List
          dataSource={indexes}
          renderItem={item=>renderIndex(item)} /> }
    </div>)

}
const renderIndex = (index)=>{
    return (
                <List.Item key={index.id}><Index  index={index} /></List.Item>
    )
  }
export default Indexes;