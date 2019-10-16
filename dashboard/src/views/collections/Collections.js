import React,{useState} from "react";
import {useGetCollections} from '../../hooks/collections';
import {List,Button,Modal } from 'antd'
import CreateDocumentForm from './create/CreateForm'

import Document from '../../components/documents/Document'
import './Collections.css'
import Loading from '../../components/loading/Loading'
import {useCreateCollection} from '../../hooks/collections'

const Collections = () => {
  const [showModal,setShowModal]=useState(false)
  const [documents,isLoadingGet] = useGetCollections([]);
  const [createdDocument, createDocument,isLoadingCreate]=useCreateCollection()
  const isLoading = isLoadingCreate || isLoadingGet
  if (isLoading && !documents) {
      return <Loading />
    }
    return (
    <div>
      <h1>Collections</h1>
      <Button type="primary" icon="plus"
      onClick={()=>{setShowModal(true)}}>
        Create
      </Button>
        {showModal && 
          <CreateDocumentForm 
          show={showModal} 
          onCancel={()=>{setShowModal(false)}} 
          onSubmit={(values)=>{
            console.log(values)
            createDocument({
              name: values.documentName,
              idKey: values.idKey,
              orderingKey: values.orderingKey,
		          active: true
            })
            setShowModal(false)
          }}
          onError={(e)=>console.error(e)}
          />}
        {!isLoading && documents &&  <List
           grid={{
            gutter: 16,
            xs: 1,
            sm: 1,
            md: 1,
            lg: 1,
            xl: 1,
            xxl: 1,
          }}
          dataSource={documents}
          renderItem={item=>document(item)} /> }

  
    </div>
  );
};

const document = (document)=>{
  return (
              <List.Item key={document.id}><Document  document={document} /></List.Item>
  )
}

export default React.memo(Collections);