import React,{useState} from "react";
import {List,Button,Modal } from 'antd'


import './Documents.css'
import Loading from '../../components/loading/Loading'
import Document from '../../components/document/Document'
import {useCreateDocument, useGetDocuments} from '../../hooks/documents'

const Documents = (props) => {
    const [showModal,setShowModal]=useState(false)
    const collectionName = props.match.params.collection
    const [documents,isLoadingGetDocuments] = useGetDocuments([]);
    //const [createdCollection, createCollection,isLoadingCreate]=useCreateCollection()
  if (isLoadingGetDocuments && !documents) {
      return <Loading />
    }
    return (
    <div>
      <h1>{collectionName}</h1>
      {/* <Button type="primary" icon="plus"
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
         */}
    {!isLoadingGetDocuments && documents &&  <List
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

export default React.memo(Documents);