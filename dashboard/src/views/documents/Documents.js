import React,{useState} from "react";
import {List,Button,Modal } from 'antd'


import './Documents.css'
import Loading from '../../components/loading/Loading'
import Documents from '../../components/document/Documents'
import {useCreateDocument, useGetDocuments} from '../../hooks/documents'
import {useGetSingleCollection} from '../../hooks/collections'

const DocumentsView = (props) => {
    const collectionName = props.match.params.collection
    const [showModal,setShowModal]=useState(false)
    const [collection,isLoadingGet] = useGetSingleCollection(collectionName,[]);
    const [documents,isLoadingGetDocuments] = useGetDocuments(collectionName,[]);
    //const [createdCollection, createCollection,isLoadingCreate]=useCreateCollection()
  if (isLoadingGetDocuments && !documents) {
      return <Loading />
    }
    return (
    <div>
      <h1>{collectionName}</h1>
    {!isLoadingGetDocuments && documents &&  <Documents collection={collection} documents={documents} /> }
  
    </div>
  );
};

export default React.memo(DocumentsView);