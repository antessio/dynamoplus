import React,{useState} from "react";
import {useGetDocuments} from '../../hooks/documents';
import {List,Button,Modal } from 'antd'
import CreateDocumentForm from './create/CreateForm'

import Document from '../../components/documents/Document'
import './Documents.css'
import Loading from '../../components/loading/Loading'
import {useCreateDocument} from '../../hooks/documents'

const Documents = () => {
  const [showModal,setShowModal]=useState(false)
  const [documents,isLoadingGet] = useGetDocuments([]);
  const [createdDocument, createDocument,isLoadingCreate]=useCreateDocument()
  const isLoading = isLoadingCreate || isLoadingGet
  if (isLoading && !documents) {
      return <Loading />
    }
    return (
    <div>
      <h1>Documents</h1>
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

export default React.memo(Documents);