import React, {useState} from 'react'

import './Indexes.css'
import Loading from '../../components/loading/Loading'
import {List,Button } from 'antd'
import {useGetIndexes} from '../../hooks/indexes'
import {useCreateIndex} from '../../hooks/indexes'
import Index from '../../components/indexes/Index'
import CreateIndexForm from './create/CreateIndexForm'
const Indexes = (props)=>{
    const documentType = props.match.params.documentType
    const [indexes,isLoading] = useGetIndexes([],documentType);
    const [indexCreated,createIndex, isLoadingCreateIndex]=useCreateIndex()
    const [showModal,setShowModal]=useState(false)
    if (isLoading && !indexes) {
        return <Loading />
      }
    return(<div>
        <h2>Indexes</h2>
        <p>{documentType}</p>
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
              document_type:{
                name: documentType
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