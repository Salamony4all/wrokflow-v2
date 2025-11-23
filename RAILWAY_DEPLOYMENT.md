# 🚀 Railway Deployment Guide

## ✅ **Your App is Ready for Railway!**

This Flask application is now configured for easy deployment on Railway.

---

## 📋 **Prerequisites**

1. **Railway Account**: Sign up at [railway.app](https://railway.app)
2. **GitHub/GitLab Repository**: Your code is already on GitLab ✅

---

## 🚀 **Quick Deploy (5 Minutes)**

### **Option 1: Deploy from GitLab (Recommended)**

1. **Login to Railway**: [railway.app](https://railway.app)
2. **New Project**: Click "New Project"
3. **Deploy from Git**: Select "Deploy from Git repo"
4. **Connect GitLab**: 
   - Authorize Railway to access GitLab
   - Select your repository: `salamony4all-group/salamony4all-project`
   - Select branch: `main`
5. **Railway Auto-Detects**:
   - ✅ Python runtime
   - ✅ `Procfile` for start command
   - ✅ `requirements.txt` for dependencies
6. **Deploy**: Railway automatically starts deploying!
7. **Get URL**: Railway provides a public URL (e.g., `https://your-app.up.railway.app`)

### **Option 2: Deploy with Railway CLI**

```bash
# Install Railway CLI
npm i -g @railway/cli

# Login
railway login

# Initialize project
railway init

# Deploy
railway up
```

---

## ⚙️ **Environment Variables**

Railway will auto-detect your app, but you may want to set:

### **Required (Optional for first deploy):**
```
SECRET_KEY=your-production-secret-key-here
FLASK_DEBUG=False
```

### **Already Set in Code:**
- ✅ PORT (Railway provides automatically)
- ✅ API_URL and TOKEN (in your code)
- ✅ File storage (uses Railway's persistent filesystem)

---

## 🔧 **Configuration Files Created**

### ✅ **Procfile**
Tells Railway how to start your app:
```
web: python app.py
```

### ✅ **runtime.txt**
Specifies Python version:
```
python-3.11.9
```

### ✅ **railway.json**
Railway deployment configuration

### ✅ **Updated app.py**
- Uses `PORT` environment variable (Railway provides)
- Disables debug mode in production
- Ready for Railway's environment

---

## 📁 **File Storage**

Railway provides **persistent filesystem**, so your app's file storage will work:
- ✅ `/uploads` directory (persists)
- ✅ `/outputs` directory (persists)
- ✅ `/flask_session` directory (persists)

**Note**: Files persist for the lifetime of your service. If you need longer-term storage, consider adding AWS S3 later.

---

## 🔍 **Post-Deployment Checklist**

1. **Test the App**:
   - Visit your Railway URL
   - Test file upload
   - Test PDF extraction
   - Test offer generation

2. **Set Custom Domain** (Optional):
   - Go to Railway Dashboard → Settings → Domains
   - Add your custom domain

3. **Monitor Logs**:
   - Railway Dashboard → Deployments → View Logs
   - Check for any errors

4. **Set Environment Variables**:
   - Railway Dashboard → Variables
   - Add `SECRET_KEY` with a strong random value

---

## 🎯 **Features That Work on Railway**

✅ **File Upload & Storage** - Works with Railway's persistent filesystem  
✅ **PDF Processing** - No timeout issues (Railway supports long-running processes)  
✅ **Session Management** - Filesystem sessions work perfectly  
✅ **Background Tasks** - Cleanup thread runs as daemon  
✅ **Static Files** - Served correctly  
✅ **All Routes** - Fully functional  

---

## 💰 **Pricing**

- **Free Tier**: $5 credit/month (plenty for testing)
- **Hobby Plan**: $5/month (500 hours included)
- **Pro Plan**: $20/month (more resources)

---

## 🆘 **Troubleshooting**

### **Build Fails**
- Check Railway logs for error messages
- Ensure `requirements.txt` is up to date
- Verify Python version compatibility

### **App Won't Start**
- Check logs in Railway dashboard
- Verify `Procfile` syntax
- Ensure PORT is being used correctly

### **File Upload Issues**
- Check file permissions
- Verify uploads directory exists
- Check disk space in Railway dashboard

---

## 🔄 **Continuous Deployment**

Railway automatically deploys when you push to your GitLab repository:
- Push to `main` branch → Auto-deploy ✅
- View deployments in Railway dashboard
- Rollback to previous versions if needed

---

## 🎉 **You're Ready!**

Your app is fully configured for Railway deployment. Just:

1. **Go to Railway** → New Project → Deploy from Git
2. **Connect your GitLab repo**
3. **Deploy!** 🚀

**Your app will be live in ~2-3 minutes!**

---

## 📚 **Resources**

- [Railway Documentation](https://docs.railway.app)
- [Railway Community](https://discord.gg/railway)
- [Support](https://railway.app/support)

