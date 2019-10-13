import {useState, useEffect} from 'react'

import { useAuth0 } from '../react-auth0-wrapper'

export const useGetDocuments = (dependencies)=>{
    const { getTokenSilently } = useAuth0();
    const [documents, setDocuments] = useState([])
    const [isLoading, setLoading] = useState(false)
    const getDocuments = async () => {
        setLoading(false)
        try {
          const token = await getTokenSilently()
          const response = await fetch(process.env.REACT_APP_API_BASE_PATH+"/dynamoplus/document_type/query/active", {
            headers: {
              Authorization: `Bearer ${token}`
            },
            method: 'POST',
            body: JSON.stringify({active: "true"})
    
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