(ns sv.gcloud.storage.client
  (:require [slingshot.slingshot :as s]
            [no.en.core :as c])
  (:import java.net.URLDecoder))

(def storage-endpoint "https://www.googleapis.com/storage/v1/b/")

(def upload-endpoint "https://www.googleapis.com/upload/storage/v1/b/")

(defn upload-request [params]
  (let [{:keys [bucket path content]} params]
    {:method :post
     :url (str (:upload-endpoint params upload-endpoint) bucket "/o")
     :query-params {:uploadType "media"
                    :name path}
     :content-type (:content-type params "application/octet-stream")
     :body content
     :as :json}))

(defn object-info-request [params]
  {:method :get
   :url (str (:storage-endpoint params storage-endpoint)
             (:bucket params) "/o/"
             (c/url-encode (:path params)))
   :as :json})

(defn get-object-info [client params]
  (s/try+
   (:body (client (object-info-request params)))
   (catch [:status 404] _
     nil)))

(defn stream-object [client params]
  (let [info (get-object-info client params)]
    (when-let [media-link (:mediaLink info)]
      (:body
       (client
        {:method :get
         :url (:mediaLink info)
         :as :stream})))))

(defn delete-object-request [params]
  (let [{:keys [bucket path content]} params]
   {:method :delete
    :url (str (:storage-endpoint params storage-endpoint)
              bucket "/o/"
              (c/url-encode path))
    :as :json}))

(defn delete-object [client params]
  (s/try+
   (:body (client (delete-object-request params)))
   (catch [:status 404] _
     false)))
