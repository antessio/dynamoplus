import React, { useState, useEffect } from "react";
import {useGetDocuments} from '../hooks/documents';
import {List } from 'antd'
import Document from '../components/documents/Document'
import './Documents.css'
import Loading from '../components/loading/Loading'

const Documents = () => {
  const [documents,isLoading] = useGetDocuments([]);
  if (isLoading && !documents) {
      return <Loading />
    }
    return (
    <div>
      <h1>Documents</h1>
        
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