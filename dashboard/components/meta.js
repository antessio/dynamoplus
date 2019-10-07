// components/meta.js
import Head from 'next/head'
export default ({ props = { title, description } }) => (
  <div>
    <Head>
     <link
            rel="stylesheet"
            // href="static/css/bootstrap.min.css"
            href="https://cdnjs.cloudflare.com/ajax/libs/antd/3.23.6/antd-with-locales.min.js"
          />
          <link
            rel="stylesheet"
            href="https://maxcdn.bootstrapcdn.com/font-awesome/4.7.0/css/font-awesome.min.css"
          />
      <title>{ props.title || 'Next.js Test Title' }</title>
      <meta name='description' content={props.description || 'Next.js Test Description'} />
      <meta name='viewport' content='width=device-width, initial-scale=1' />
      <meta charSet='utf-8' />
    </Head>
  </div>
)
