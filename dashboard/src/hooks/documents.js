import {useState, useEffect} from 'react'

import { useAuth0 } from '../react-auth0-wrapper'



export const useCreateDocument = (collection)=>{
    const { getTokenSilently } = useAuth0();
    const [document, setDocument] = useState(null)
    const [isLoading, setLoading] = useState(false)
    const createDocument = async (d) => {
      setLoading(false)
      try {
        const token = await getTokenSilently()
        const response = await fetch(process.env.REACT_APP_API_BASE_PATH+"/dynamoplus/"+collection, {
          headers: {
            Authorization: `Bearer ${token}`
          },
          method: 'POST',
          body: JSON.stringify(d)
  
        });
  
        const responseData = await response.json();
        console.log(responseData)
        setDocument(responseData)
        setLoading(false)
      } catch (error) {
        setLoading(false)
      }
    }; 
    return [document, createDocument, isLoading]
}

export const useGetDocuments = (collectionName,dependencies)=>{
    const { getTokenSilently } = useAuth0();
    const [documents, setDocuments] = useState([])
    const [isLoading, setLoading] = useState(false)
    const getDocuments = async () => {
        setLoading(true)
        try {
          const token = await getTokenSilently()
          const response = await fetch(process.env.REACT_APP_API_BASE_PATH+"/dynamoplus/"+collectionName+"/query", {
            headers: {
              Authorization: `Bearer ${token}`
            },
            method: 'POST',
            body: JSON.stringify({})
    
          });
    
          const responseData = await response.json();
          
          const documents = responseData.data
          console.log(documents)
          setDocuments(documents)
          setLoading(false)
        } catch (error) {
          setLoading(false)
        }
      };
    useEffect(() => {
        //setLoading(true)
        getDocuments()
      }, dependencies);
    return [documents,isLoading]


}